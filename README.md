# Apartments for sale - Otodom property scraper & database manager 

## 🏡 About

This project is a web scraping tool designed to automatically collect real estate data from Otodom, a popular property listing platform. The scraper automates the process of extracting property listings and their associated data, which is then stored in a PostgreSQL database for further analysis

🔄 Status of the project: in progress

The project is currently tailored for scraping only real estate listings related to apartment sales in a specific city

⚠️ **Built only for personal use, for learning and portfolio purposes. I do not recommend using this code for anything other than learning**


## 📦 Database Structure

The database is designed to store apartment listings data, price history, photos, and extracted features. It consists of the following tables:

- `locations` – stores unique location details (city, district, street, etc)
- `apartments_sale_listings` – main table for apartment data
- `price_history` – stores historical price changes
- `photos` – stores binary image data (BYTEA type) related to listings
- `features` – extracted flat features (e.g. air conditioning, balcony, parking, etc)

💡 You can preview the structure in `db/schema.sql`

⚠️ It is designed primarily to work with Katowice listings on Otodom. Therefore, the structure of the locations table assumes expansion to other cities, but still within the Silesian region. The scraper will work for other cities and voivodeships, but the database may not be optimally structured. For future expansion, it is recommended to split the locations table into smaller parts, such as separate tables for voivodeships, cities and/or districts.

![Database Structure](imgs/db_structure.png)

## 🛠 Database Setup

Required **PostgreSQL** server running and a database named `apartments_for_sale` created:

```bash
psql -U postgres
CREATE DATABASE apartments_for_sale;
```


## 🔑 Required Environment Variables

Required a .env file in the project root with your PostgreSQL credentials:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=apartments_for_sale
DB_USER=postgres
DB_PASSWORD=your_password
```

💡 When running main.py, the necessary tables will be automatically created if they don't already exist, so you don't need to manually handle the table setup.
The imported in main.py db/db_setup.py module handles database connection and table creation. It checks if any required tables already exist. If not, it reads SQL commands from schema.sql and creates them (in case of starting it all again, it is better to drop all tables created before, as code check only if any required table exists)


## 📝 Logging

Database operations are logged using Python's logging module. Logs are saved to the logs/ directory and can be adjusted via config/logging_config.py


