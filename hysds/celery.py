from future import standard_library

standard_library.install_aliases()
from celery import Celery

app = Celery("hysds")
app.config_from_object("celeryconfig")


if __name__ == "__main__":
    app.start()
