pub fn init_logger() {
    // println!("current dir: {:?}", std::env::current_dir());
    log4rs::init_file("./log_config.yml", Default::default()).expect("Couldn't initialize logger")
}
