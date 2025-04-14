CREATE TABLE locations (
    id SERIAL PRIMARY KEY, -- ID Tabeli locations
    voivodeship TEXT,  -- Województwo
    city TEXT,  -- Miasto
    district TEXT  -- Dzielnica
);

CREATE TABLE apartments_sale_listings (
    id SERIAL PRIMARY KEY,
    otodom_listing_id BIGINT, -- ID oferty (z otodom)
    title TEXT, -- Tytuł
    market TEXT, -- Rynek (pierwotny, wtórny)
    advert_type TEXT, -- Rodzaj ogłoszenia (prywatne/agencja)
    creation_date DATE, -- Data utworzenia oferty
    creation_time TEXT,  -- Godzina utworzenia oferty
    pushed_ap_at DATE,  -- Data 'wypchania' na górę (promowanie, odświezenie, algorytm olx)
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
    owner_id BIGINT,  -- ID właściciela oferty
    owner_name TEXT,  -- Imię właściciela oferty
    agency_id BIGINT,  -- ID agencji, może być NULL
    agency_name TEXT, -- Nazwa agencji
    offer_link TEXT,  -- Link do oferty 
    active BOOLEAN,  -- Status oferty (czy aktualna)
    closing_date DATE, -- Data zniknięcia oferty (sprzedaz, usunięcie, wygaśnięcie)
    FOREIGN KEY(location_id) REFERENCES locations(id)
);

CREATE TABLE price_history (
    id SERIAL PRIMARY KEY, -- ID tabeli price_history
    listing_id BIGINT, -- ID oferty
    old_price INT, -- Poprzednia cena
    new_price INT, -- Nowa cena
    change_date DATE, -- Data dokonania zmiany
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

