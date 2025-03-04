import yaml 
def load_config(config_path):
    print("Processing Loading Configuration.....")
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    print("Configuration Finished Load")
    return config