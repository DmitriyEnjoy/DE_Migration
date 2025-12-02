-- init_source.sql
CREATE TABLE IF NOT EXISTS public.flowers (
  id serial PRIMARY KEY,
  name text,
  color text,
  created_at timestamp default now()
);

INSERT INTO public.flowers (name, color) VALUES
  ('rose', 'red'),
  ('tulip', 'yellow')
ON CONFLICT DO NOTHING;
