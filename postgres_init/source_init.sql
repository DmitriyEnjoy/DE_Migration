CREATE TABLE IF NOT EXISTS public.users (
  id serial PRIMARY KEY,
  name varchar(100),
  email varchar(255),
  created_at timestamptz default now()
);

INSERT INTO public.users (name, email) VALUES ('Alice','alice@example.com'), ('Bob','bob@example.com');
