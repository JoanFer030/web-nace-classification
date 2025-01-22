import yaml
from data.extract import extract_data
from data.transform import transform_data
from data.feature_creation import create_feature


def main():
    with open("config/parameters.yaml", "r") as file:
        config = yaml.safe_load(file)
    
    print("\nData Extraction Process Initialized")
    print("-"*60)
    extract_data(config)

    print("\nData Transformation Process Initialized")
    print("-"*60)
    transform_data(config)

    print("\nFeature Creation Process Initialized")
    print("-"*60)
    create_feature(config)


if __name__ == "__main__":
    main()