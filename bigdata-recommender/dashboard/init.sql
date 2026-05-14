-- Create recommender database and tables
CREATE DATABASE recommender;

\c recommender;

CREATE TABLE IF NOT EXISTS recommendations (
    id               SERIAL PRIMARY KEY,
    user_id          VARCHAR(255) NOT NULL,
    product_id       VARCHAR(255) NOT NULL,
    predicted_rating FLOAT,
    epoch_id         BIGINT,
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_recommendations_user_id ON recommendations(user_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_created_at ON recommendations(created_at);

-- Model metrics table
CREATE TABLE IF NOT EXISTS model_metrics (
    id         SERIAL PRIMARY KEY,
    rmse       FLOAT,
    rank       INT,
    max_iter   INT,
    reg_param  FLOAT,
    train_count BIGINT,
    test_count  BIGINT,
    created_at TIMESTAMP DEFAULT NOW()
);
