use std::{fmt, num::NonZeroUsize};

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use prost::Message;
use tower::Service;
use vector_lib::request_metadata::GroupedCountByteSize;
use vector_lib::stream::{batcher::data::BatchReduce, BatcherSettings, DriverResponse};
use vector_lib::{config::telemetry, ByteSizeOf, EstimatedJsonEncodedSizeOf};

use super::service::MyVectorRequest;
use crate::{
    event::{proto::EventWrapper, Event, EventFinalizers, Finalizable},
    proto::vector as proto_vector,
    sinks::util::{metadata::RequestMetadataBuilder, SinkBuilderExt, StreamSink},
};

/// Data for a single Kafka message.
struct KafkaMessage {
    byte_size: usize,
    json_byte_size: GroupedCountByteSize,
    finalizers: EventFinalizers,
    wrapper: EventWrapper,
    topic: String,
    key: Option<Vec<u8>>,
}

/// Temporary struct to collect Kafka messages during batching.
#[derive(Clone)]
struct KafkaMessages {
    pub finalizers: EventFinalizers,
    pub messages: Vec<EventWrapper>,
    pub messages_byte_size: usize,
    pub messages_json_byte_size: GroupedCountByteSize,
    pub topic: String,
}

impl Default for KafkaMessages {
    fn default() -> Self {
        Self {
            finalizers: Default::default(),
            messages: Default::default(),
            messages_byte_size: Default::default(),
            messages_json_byte_size: telemetry().create_request_count_byte_size(),
            topic: String::new(),
        }
    }
}

pub struct MyVectorSink<S> {
    pub batch_settings: BatcherSettings,
    pub service: S,
}

impl<S> MyVectorSink<S>
where
    S: Service<MyVectorRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send,
{
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        input
            .map(|mut event| {
                let mut byte_size = telemetry().create_request_count_byte_size();
                byte_size.add_event(&event, event.estimated_json_encoded_size_of());

                // Extract topic and key from the event
                let topic = event.metadata().get("topic").unwrap_or("default").to_string();
                let key = event.metadata().get("key").map(|k| k.as_bytes().to_vec());

                KafkaMessage {
                    byte_size: event.size_of(),
                    json_byte_size: byte_size,
                    finalizers: event.take_finalizers(),
                    wrapper: EventWrapper::from(event),
                    topic,
                    key,
                }
            })
            .batched(self.batch_settings.as_reducer_config(
                |data: &KafkaMessage| data.wrapper.encoded_len(),
                BatchReduce::new(|kafka_messages: &mut KafkaMessages, item: KafkaMessage| {
                    kafka_messages.finalizers.merge(item.finalizers);
                    kafka_messages.messages.push(item.wrapper);
                    kafka_messages.messages_byte_size += item.byte_size;
                    kafka_messages.messages_json_byte_size += item.json_byte_size;
                    kafka_messages.topic = item.topic;
                }),
            ))
            .map(|kafka_messages| {
                let builder = RequestMetadataBuilder::new(
                    kafka_messages.messages.len(),
                    kafka_messages.messages_byte_size,
                    kafka_messages.messages_json_byte_size,
                );

                let encoded_events = proto_vector::PushEventsRequest {
                    events: kafka_messages.messages,
                };

                let byte_size = encoded_events.encoded_len();
                let bytes_len =
                    NonZeroUsize::new(byte_size).expect("payload should never be zero length");

                MyVectorRequest {
                    finalizers: kafka_messages.finalizers,
                    metadata: builder.with_request_size(bytes_len),
                    request: encoded_events,
                }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait]
impl<S> StreamSink<Event> for MyVectorSink<S>
where
    S: Service<MyVectorRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
