ALTER TABLE staging.user_order_log ADD COLUMN IF NOT EXISTS status varchar(100) NOT NULL DEFAULT 'shipped';

ALTER TABLE mart.f_sales ADD COLUMN IF NOT EXISTS status varchar(100) NOT NULL DEFAULT 'shipped';