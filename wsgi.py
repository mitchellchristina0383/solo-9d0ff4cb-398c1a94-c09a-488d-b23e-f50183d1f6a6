"""gunicorn / 本地 python wsgi.py 入口。"""

from app import create_app

application = create_app()
app = application

if __name__ == "__main__":
    application.run(host="0.0.0.0", port=5000, debug=False)
