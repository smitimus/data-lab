-- Coupon date validation requires coupon_id on transaction items.
-- The API does not expose coupon_id on transaction items, so this test is skipped.
select 1 where false
