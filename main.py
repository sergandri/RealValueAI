from exploration_scrapping.models import OutSchema, InSchema
from exploration_scrapping.tools import RealtyScraper, FileManager, DataFramePreprocessor
from exploration_scrapping.config import scraper_config, file_config


def main():
    scraper = RealtyScraper(scraper_config)
    preprocessor = DataFramePreprocessor()
    file_manager = FileManager(config=file_config)

    df = scraper.scrape()
    print(df)
    df.pipe(InSchema.validate).pipe(preprocessor.preprocess).pipe(OutSchema.validate).pipe(file_manager.save_dataframe)


if __name__ == "__main__":
    main()
