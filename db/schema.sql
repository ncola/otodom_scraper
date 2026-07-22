CREATE TABLE locations (
    id SERIAL PRIMARY KEY, -- ID Tabeli locations
    voivodeship TEXT,  -- Województwo
    city TEXT,  -- Miasto
    district TEXT  -- Dzielnica
);

-- APARTMENTS

CREATE TABLE apartments_sale_listings (
    id SERIAL PRIMARY KEY,
    otodom_listing_id BIGINT, -- ID oferty (z otodom)
    title TEXT, -- Tytuł
    market TEXT, -- Rynek (pierwotny, wtórny)
    advert_type TEXT, -- Rodzaj ogłoszenia (prywatne/agencja)
    creation_date DATE, -- Data utworzenia oferty
    creation_time TEXT,  -- Godzina utworzenia oferty
    pushed_up_at DATE,  -- Data 'wypchania' na górę (promowanie, odświezenie, algorytm olx)
    exclusive_offer BOOLEAN, -- Czy wyróznione
    creation_source TEXT, -- Sposób wprowadzenia oferty (ręcznie, poprzez API)
    description_text TEXT, -- Opis
    area NUMERIC(10, 2),  -- Powierzchnia
    price BIGINT,  -- Cena
    updated_price BIGINT, -- Cena aktualna
    price_per_m NUMERIC(10, 2),  -- Cena za metr kwadratowy
    updated_price_per_m NUMERIC(10,2), -- Cena za metr kwadratowy aktualna
    location_id BIGINT, -- ID lokalizacji (od województwa do dzielnicy)
    street TEXT, -- Ulica, często unikalna wartość, jest ich duzo więcej, dlatego nie jest w locations
    rent_amount INT,  -- Wysokość czynszu
    rooms_num INT,  -- Liczba pokoi
    floor_num VARCHAR(5),  -- Numer piętra, varchar poniewa
    heating TEXT,  -- Typ ogrzewania
    ownership TEXT,  -- Rodzaj własności
    proper_type TEXT,  -- Typ nieruchomości (dla potwierdzenia)
    construction_status TEXT,  -- Status (np do remontu, w budowie)
    energy_certificate TEXT,  -- Certyfikat energetyczny
    building_build_year INT,  -- Rok budowy budynku
    building_floors_num INT,  -- Liczba pięter w budynku
    building_material TEXT,  -- Materiał budynku
    building_type TEXT,  -- Typ budynku
    windows_type TEXT,  -- Rodzaj okien
    local_plan_url TEXT,  -- URL do planu lokalnego
    video_url TEXT,  -- URL do wideo
    view3d_url TEXT,  -- URL do widoku 3D
    walkaround_url TEXT,  -- URL do spaceru
    development_id BIGINT, -- ID inwestycji, może być NULL
    development_title TEXT, -- Nazwa inwestycji, może być NULL
    owner_id BIGINT,  -- ID właściciela oferty
    owner_name TEXT,  -- Imię właściciela oferty
    agency_id BIGINT,  -- ID agencji, może być NULL
    agency_name TEXT, -- Nazwa agencji
    offer_link TEXT,  -- Link do oferty 
    active BOOLEAN,  -- Status oferty (czy aktualna)
    detected_inactive_at DATE, -- Data zniknięcia oferty (sprzedaz, usunięcie, wygaśnięcie)
    db_created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    db_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(location_id) REFERENCES locations(id)
);

CREATE TABLE price_history (
    id SERIAL PRIMARY KEY, -- ID tabeli price_history
    listing_id INT, -- ID oferty
    old_price BIGINT, -- Poprzednia cena
    new_price BIGINT, -- Nowa cena
    change_date DATE, -- Data dokonania zmiany
    db_created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (listing_id) REFERENCES apartments_sale_listings(id) -- Ustanowienie ID oferty kluczem obcym 
);

CREATE TABLE photos (
    id SERIAL PRIMARY KEY, -- ID tabeli photos
    listing_id BIGINT, -- ID oferty
    photo BYTEA, -- Zdjęcie
    FOREIGN KEY (listing_id) REFERENCES apartments_sale_listings(id) -- Ustanowienie ID oferty kluczem obcym 
);

CREATE TABLE features ( -- Oznaczenie cech mieszkania, jeden wiersz == jedno ogłoszenie
    listing_id BIGINT PRIMARY KEY, --  
    internet BOOLEAN,
    cable_television BOOLEAN,
    phone BOOLEAN,
    roller_shutters BOOLEAN,
    anti_burglary_door BOOLEAN,
    entryphone BOOLEAN,
    monitoring BOOLEAN,
    alarm BOOLEAN,
    closed_area BOOLEAN,
    furniture BOOLEAN,
    washing_machine BOOLEAN,
    dishwasher BOOLEAN,
    fridge BOOLEAN,
    stove BOOLEAN,
    oven BOOLEAN,
    tv BOOLEAN,
    balcony BOOLEAN,
    usable_room BOOLEAN,
    garage BOOLEAN,
    basement BOOLEAN,
    garden BOOLEAN,
    terrace BOOLEAN,
    lift BOOLEAN,
    two_storey BOOLEAN,
    separate_kitchen BOOLEAN,
    air_conditioning BOOLEAN,
    FOREIGN KEY (listing_id) REFERENCES apartments_sale_listings(id)  
);

-- indexes for performance optimization
CREATE UNIQUE INDEX IF NOT EXISTS ux_listing_otodom_id
ON apartments_sale_listings (otodom_listing_id);

CREATE INDEX IF NOT EXISTS ix_listing_location
ON apartments_sale_listings (location_id);

CREATE INDEX IF NOT EXISTS ix_price_history_listing
ON price_history (listing_id);

CREATE INDEX IF NOT EXISTS ix_features_listing
ON features (listing_id);

CREATE INDEX IF NOT EXISTS ix_photos_listing
ON photos (listing_id);


-- PLOTS

CREATE TABLE IF NOT EXISTS plots_sale_listings (
    id BIGSERIAL PRIMARY KEY,
    otodom_listing_id BIGINT NOT NULL UNIQUE,
    offer_link TEXT NOT NULL,
    source_status TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    detected_inactive_at TIMESTAMPTZ,
    title TEXT,
    description_text TEXT,
    market TEXT,
    advert_type TEXT,
    advertiser_type TEXT,
    creation_source TEXT,
    creation_at TIMESTAMPTZ,
    modified_at TIMESTAMPTZ,
    pushed_up_at TIMESTAMPTZ,
    exclusive_offer BOOLEAN,
    area NUMERIC(14, 2),
    price NUMERIC(16, 2),
    updated_price NUMERIC(16, 2),
    price_per_m NUMERIC(14, 2),
    updated_price_per_m NUMERIC(14, 2),
    location_id INTEGER REFERENCES locations(id),
    street TEXT,
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    plot_types TEXT[],
    dimensions TEXT,
    fence TEXT,
    media_types TEXT[],
    access_types TEXT[],
    vicinity_types TEXT[],
    owner_id BIGINT,
    owner_name TEXT,
    agency_id BIGINT,
    agency_name TEXT,
    local_plan_url TEXT,
    video_url TEXT,
    view3d_url TEXT,
    walkaround_url TEXT,
    source_target JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_characteristics JSONB NOT NULL DEFAULT '[]'::jsonb,
    db_created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    db_updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS plots_price_history (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES plots_sale_listings(id),
    old_price NUMERIC(16, 2),
    new_price NUMERIC(16, 2),
    change_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_plots_location_active
ON plots_sale_listings (location_id, active);

CREATE INDEX IF NOT EXISTS ix_plots_active
ON plots_sale_listings (active);

CREATE INDEX IF NOT EXISTS ix_plots_price_history_listing
ON plots_price_history (listing_id);

CREATE INDEX IF NOT EXISTS ix_plots_media_types
ON plots_sale_listings USING GIN (media_types);

CREATE INDEX IF NOT EXISTS ix_plots_source_target
ON plots_sale_listings USING GIN (source_target);
