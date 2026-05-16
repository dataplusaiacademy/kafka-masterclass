-- Logical decoding + publication for Debezium (pgoutput).
CREATE TABLE public.orders (
    id          SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    qty         INT NOT NULL DEFAULT 1,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE public.orders REPLICA IDENTITY FULL;

CREATE PUBLICATION dbz_orders FOR TABLE public.orders;

INSERT INTO public.orders (product_name, qty) VALUES ('seed-order', 1);
