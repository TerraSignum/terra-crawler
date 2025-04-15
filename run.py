from terra_crawler_system import app, start_all_project_schedulers

if __name__ == "__main__":
    start_all_project_schedulers(interval_minutes=1)
    app.run(debug=True, host="0.0.0.0", port=5000)
