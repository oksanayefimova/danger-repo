library(duckdb)
library(duckplyr, warn.conflicts = FALSE)
library(sf)
library(tidyverse)
library(janitor)
library(plotly)
library(scales)
library(extrafont)

# con <- dbConnect(duckdb())
# 
# dbExecute(con, "
#   COPY (
#     SELECT * FROM read_csv_auto('311_Service_Requests_2024_2025.csv', all_varchar = TRUE)
#   ) TO '311_Service_Requests_2024_2025.parquet' (FORMAT PARQUET)
# ")
# 
# dbDisconnect(con) 


# The broken windows theory is a criminological concept suggesting that 
# visible signs of disorder, like broken windows, graffiti, and litter,
# create an environment that encourages more serious crime. The theory 
# proposes that by aggressively policing minor offenses, authorities 
# can maintain public order and prevent more significant criminal 
# activity. It gained prominence through its application in cities like
# New York during the 1990s. 

nyc_streets <- read_sf("~/Downloads/DCM_StreetNameChanges_Points_20251019 (1)/geo_export_67db49cf-9b2b-47e5-9ab1-d7311aba0766.shp")
head(nyc_streets)

nyc_map <- read_sf("~/Downloads/nybb_25c/nybb.shp")
nyc_map <- nyc_map %>% 
  mutate(BoroName = tolower(BoroName))
head(nyc_map)

requests_data <- read_parquet_duckdb("~/Downloads/311_Service_Requests_2024_2025.parquet") #важливо!!! дані за 2024-2025 рік бо якщо брати все це 41 млн рядків й воно просто в мене ніяк не скачалось якщо треба буде можу ще окремо взяти ще якийсь рік
head(requests_data)
colnames(requests_data)
requests_data %>% select("X Coordinate (State Plane)", "Y Coordinate (State Plane)")
# Non-emergency municipal services 2024 - 2025

сomplaints_data <- read_parquet_duckdb("~/Downloads/nyc_complaints.parquet")
# Valid felony, misdemeanor, and violation crimes reported to the New York City Police Department (NYPD) from 2006 to the end of last year (2019). 
head(сomplaints_data)
сomplaints_data %>% select(X_COORD_CD, Y_COORD_CD)

colnames(сomplaints_data)

library(dplyr)

# base dataframe
suspects <- сomplaints_data |>
  select(CMPLNT_NUM, SUSP_AGE_GROUP, SUSP_RACE, SUSP_SEX, LAW_CAT_CD, OFNS_DESC, BORO_NM, CMPLNT_FR_DT, X_COORD_CD, Y_COORD_CD) |>
  collect() |>
  filter(!is.na(SUSP_AGE_GROUP), SUSP_AGE_GROUP != "UNKNOWN", !is.na(X_COORD_CD), !is.na(Y_COORD_CD)) |>
  mutate(
    CMPLNT_FR_DT = as.Date(CMPLNT_FR_DT, format = "%m/%d/%Y"),
    X_COORD_CD = as.numeric(gsub(",", "", X_COORD_CD)),
    Y_COORD_CD = as.numeric(gsub(",", "", Y_COORD_CD))) |>
  filter(CMPLNT_FR_DT >= as.Date('2024-01-01') & CMPLNT_FR_DT <= as.Date('2025-12-31')) |>
  clean_names()

suspects_dt <- suspects %>% 
  count(cmplnt_fr_dt)
# фільтр по роках бо якщо далі якось порівнювати ці дві таюлиці то не спіпадіння в періодах впливаж на результат
ggplot(suspects_dt, aes(x = cmplnt_fr_dt, y = n)) + geom_line()
# pyramids on every crime type
suspect_age <- suspects |> 
  filter(susp_age_group != "(null)", boro_nm != "Undefined", boro_nm != "(null)") |> 
  group_by(law_cat_cd, susp_age_group, boro_nm) |> 
  summarise(count = n()) |> 
  filter(grepl("\\d+-\\d+", susp_age_group)) |>
  collect() #результат виводу вікова група категорія злочину скільки людей в цій віковій групі зробили цей типу злочину й відсоток відносно своєї вікової групи
suspect_age

# 1
ggplot(suspect_age, aes(fill=susp_age_group, y=count, x=law_cat_cd)) + 
  geom_bar(position="fill", stat="identity") + 
  facet_wrap(~ boro_nm, scale = "free_x") +
  theme_minimal() +
  scale_fill_viridis_d(option = "cividis", direction = -1) +
  labs(title = "Crime severity by borough", subtitle = "(2024 - 2025)", x = "Crime severity",
       y = NULL, fill = "Age", caption = "Source: NYC Open Data") +
  theme(plot.title = element_text(face = "bold", size = 18),
        text = element_text(family = "sans"),
        plot.background = element_rect(fill = "#fcfbfa", color = NA), 
        plot.margin = margin_auto(1, unit = "cm"), 
        axis.text = element_text(size = 8))



# nyc map
location <- suspects |> 
  filter(boro_nm != "(null)") |> 
  group_by(boro_nm) |> 
  summarise(total_cases = n()) |> 
  arrange(desc(total_cases)) |> #райони й кількість злочинів
  mutate(boro_nm = tolower(boro_nm)) |>
  collect()


# loc_map <- nyc_map %>% 
#   left_join(location, by = c("BoroName" = "boro_nm"))
# class(loc_map)
# loc_map
# class(loc_map$total_cases)

# loc <- ggplot() +
#   geom_sf(data = loc_map, aes(fill = total_cases, text = paste0("Borough: ", BoroName)), color = "white", size = 0.15) + 
#   coord_sf() +
#   scale_fill_viridis_c(option = "magma", direction = 1, name = "Total Cases", trans = "sqrt") +
#   theme_void()
# ggplotly(loc, tooltip = "text")

suspect_gender <- suspects |> 
  filter(susp_sex != "(null)") |> 
  group_by(susp_sex) |> 
  summarise(total = n()) |> 
  mutate(percentage = total / sum(total) * 100) |> #стать підозрюваних й кількість злочинів
  collect()
suspect_gender


#по суті аналогічно можна зробити для постраждалого 

# requests_location <- requests_data |>
#   filter(Borough != "(null)", Borough != "Unspecified") |>
#   compute(prudence = "lavish") |>
#   summarise(total_requests = n(), .by = Borough) |>
#   arrange(desc(total_requests)) |>
#   collect() #райони й кількість запитів 
# requests_location

requests_loc <- requests_data %>% 
  clean_names() %>% 
  select(x_coordinate_state_plane, y_coordinate_state_plane) %>% 
  collect() %>% 
  mutate(
    x_coordinate_state_plane = as.numeric(gsub(",", "", x_coordinate_state_plane)),
    y_coordinate_state_plane = as.numeric(gsub(",", "", y_coordinate_state_plane))) %>% 
  filter(!is.na(x_coordinate_state_plane), !is.na(y_coordinate_state_plane)) %>% 
  st_as_sf(coords = c("x_coordinate_state_plane", "y_coordinate_state_plane"), crs = 2263)

requests_loc

suspects_map <- suspects %>% 
  st_as_sf(coords = c("x_coord_cd", "y_coord_cd"), crs = 2263)
nyc_map %>% st_transform(2263)



coords_m <- bind_rows(
  requests_loc %>%  select(geometry) %>% mutate(source = "311 Requests"),
  suspects_map %>%  select(geometry) %>% mutate(source = "NYPD Suspects")) %>% 
  filter(st_coordinates(.)[, "X"] > 900000, 
         st_coordinates(.)[, "X"] < 1100000,
         st_coordinates(.)[, "Y"] > 100000, 
         st_coordinates(.)[, "Y"] < 300000)

coords <- coords_m %>% 
  st_transform(4326) %>% 
  st_coordinates() %>%
  as_tibble() %>% 
  mutate(source = coords_m$source)
coords

# 2
ggplot() + 
  geom_hex(data = coords, aes(x = X, y = Y, fill = after_stat(count)), bins = 40) + 
  facet_wrap(~ source) +
  geom_sf(data = st_transform(nyc_map, 4326), fill = NA, color = "white", alpha = 0.2, linewidth = 0.2) +
  scale_fill_viridis_c(option = "cividis", direction = -1, trans = "log", labels = label_number(accuracy = 1)) +
  theme_void() + 
  labs(title = "311 complaints and violations comparison", subtitle = "NYC map", fill = "Count",
       caption = "Source: NYC Open Data") +
  theme(plot.title = element_text(face = "bold", size = 18),
        text = element_text(family = "sans"),
        plot.background = element_rect(fill = "#fcfbfa", color = NA), 
        plot.margin = margin_auto(1, unit = "cm"))

location_comparison <- location |>
  inner_join(requests_location, by = c("boro_nm" = "Borough")) |>
  mutate(
    ratio = total_cases / total_requests
  ) |>
  arrange(desc(ratio))
location_comparison

unique_requests <- requests_data |>
  filter(Borough != "Unspecified") %>% 
  collect() %>% 
  mutate(`Complaint Type` = str_split_i(`Complaint Type`, pattern = " - ", i = 1)) %>% 
  group_by(Borough, `Complaint Type`) |>
  summarise(n = n(), .groups = 'drop') |>
  arrange(Borough, -n) %>% 
  group_by(Borough) %>% 
  slice_head(n = 5)

unique_requests <- unique_requests %>% 
  mutate(n = n/1000, `Complaint Type` = tolower(`Complaint Type`), 
         case_when(`Complaint Type` == "homeless person assistance " ~ "homeless assistance",
                   T ~ `Complaint Type`))
#  distinct() #не знаю можна зробить топ дебільних запитів або статистика скільки дивних запитів можна почути за день
unique_requests

# 3
ggplot(unique_requests, aes(x = n, y = fct_reorder(`Complaint Type`, n), fill = Borough)) + 
  geom_col() +
  facet_wrap(~ Borough, scale = "free") +
  scale_fill_viridis_d(option = "cividis", direction = -1) +
  theme_minimal() + 
  labs(title = "Common 311 complaints by borough", subtitle = "(2024 - 2025)", fill = "Borough",
       caption = "Source: NYC Open Data", y = NULL, x = "cases, k") +
  theme(plot.title = element_text(face = "bold", size = 18),
        text = element_text(family = "sans"),
        plot.background = element_rect(fill = "#fcfbfa", color = NA), 
        plot.margin = margin_auto(1, unit = "cm"),
        axis.text = element_text(size = 8))



# crime_requests <- requests_data |>
#   select(`Complaint Type`, `Created Date`, Borough) |>
#   compute(prudence = "lavish") |>
#   collect() |> 
#   filter(grepl("crime|assault|theft|robbery|burglary|arrest", `Complaint Type`, ignore.case = TRUE)) |>
#   group_by(`Complaint Type`, Borough) |>
#   summarise(count = n(), .groups = "drop") |>
#   arrange(desc(count)) #не катіт взагалі в complaints немає нічого супер кримінального, максимум що хтось може бачив наркоту

