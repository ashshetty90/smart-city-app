from app.app import SmartCityApp
import logging

# initialise loggers
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

if __name__ == '__main__':
    """
       Entry point for the Smart City application
    """
    logger.info("Application starting in main")
    smart_city_app = SmartCityApp()
    smart_city_app.run()
    logger.info("Application completed successfully")
