fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .format(true)
        .compile(&["proto/search.proto"], &["proto"])?;
    Ok(())
}
