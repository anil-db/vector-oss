use std::io::Error;

fn main() -> Result<(), Error> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(
            &[
                "src/proto/kafkaproducerproxy/kafkaproducerproxy.proto",
            ],
            &["src/proto/kafkaproducerproxy"],
        )?;

    Ok(())
}
