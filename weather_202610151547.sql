CREATE TABLE IF NOT EXISTS public.weather (
	 city VARCHAR(255) NOT NULL,
	 average_temperature DOUBLE PRECISION NOT NULL
	);

INSERT INTO public.weather (city,average_temperature) VALUES
	 ('Bangkok',31.5),
	 ('HaNoi',31.5),
	 ('HoChiMinh',31.5),
	 ('HaNoi',31.5),
	 ('Bangkok',31.5),
	 ('HoChiMinh',31.5);
