-- ─────────────────────────────────────────────────────────────────────────────
-- UK Trade Flow schema
-- ─────────────────────────────────────────────────────────────────────────────

-- Dimension: countries
CREATE TABLE IF NOT EXISTS dim_countries (
    country_code  CHAR(2)      PRIMARY KEY,
    country_name  VARCHAR(255) NOT NULL,
    region        VARCHAR(100),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Dimension: commodities
CREATE TABLE IF NOT EXISTS dim_commodities (
    commodity_code VARCHAR(20)  PRIMARY KEY,
    commodity_desc TEXT,
    section_code   VARCHAR(10),
    section_desc   TEXT,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Fact: trade flows
CREATE TABLE IF NOT EXISTS trade_flows (
    id             BIGSERIAL    PRIMARY KEY,
    period         DATE         NOT NULL,           -- first day of the reporting month
    commodity_code VARCHAR(20)  NOT NULL,
    commodity_desc TEXT,
    flow_type      VARCHAR(50)  NOT NULL,           -- e.g. EU_IMPORTS, NON_EU_EXPORTS
    country_code   CHAR(2)      NOT NULL,
    country_name   VARCHAR(255),
    value_gbp      NUMERIC(18, 2),
    net_mass_kg    NUMERIC(18, 3),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- Upsert key — one record per period × commodity × flow × country
    CONSTRAINT uq_trade_flows UNIQUE (period, commodity_code, flow_type, country_code)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_tf_period          ON trade_flows (period);
CREATE INDEX IF NOT EXISTS idx_tf_commodity_code  ON trade_flows (commodity_code);
CREATE INDEX IF NOT EXISTS idx_tf_country_code    ON trade_flows (country_code);
CREATE INDEX IF NOT EXISTS idx_tf_flow_type       ON trade_flows (flow_type);
