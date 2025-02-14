from src.update_config import ConfigUpdater

if __name__ == "__main__":
    print("Starting the config update process...")
    updater = ConfigUpdater()
    updater.update_config_files()
    print("Config update completed successfully.")
