library(duckdb)
library(duckplyr, warn.conflicts = FALSE)

# con <- dbConnect(duckdb())
# 
# dbExecute(con, "
#   COPY (
#     SELECT * FROM read_csv_auto('311_Service_Requests_2024_2025.csv', all_varchar = TRUE)
#   ) TO '311_Service_Requests_2024_2025.parquet' (FORMAT PARQUET)
# ")
# 
# dbDisconnect(con) 


requests_data <- read_parquet_duckdb("311_Service_Requests_2024_2025.parquet") #важливо!!! дані за 2024-2025 рік бо якщо брати все це 41 млн рядків й воно просто в мене ніяк не скачалось якщо треба буде можу ще окремо взяти ще якийсь рік
head(requests_data)
colnames(requests_data)

сomplaints_data <- read_parquet_duckdb("nyc_complaints.parquet")
head(сomplaints_data)
colnames(сomplaints_data)

library(dplyr)

suspects <- сomplaints_data |>
  select(CMPLNT_NUM, SUSP_AGE_GROUP, SUSP_RACE, SUSP_SEX, LAW_CAT_CD, OFNS_DESC, BORO_NM, CMPLNT_FR_DT, X_COORD_CD, Y_COORD_CD) |>
  collect() |>
  filter(!is.na(SUSP_AGE_GROUP) & SUSP_AGE_GROUP != "UNKNOWN") |>
  mutate(
    CMPLNT_FR_DT = as.Date(CMPLNT_FR_DT, format = "%m/%d/%Y"),
    X_COORD_CD = as.numeric(X_COORD_CD),
    Y_COORD_CD = as.numeric(Y_COORD_CD)
  ) |>
  filter(CMPLNT_FR_DT >= as.Date('2024-01-01') & CMPLNT_FR_DT <= as.Date('2025-12-31')) # фільтр по роках бо якщо далі якось порівнювати ці дві таюлиці то не спіпадіння в періодах впливаж на результат

suspect_age <- suspects |> 
  filter(SUSP_AGE_GROUP != "(null)") |> 
  group_by(SUSP_AGE_GROUP, LAW_CAT_CD) |> 
  summarise(count = n()) |> 
  mutate(percentage = count / sum(count) * 100) |> 
  arrange(desc(count)) |> 
  collect() #результат виводу вікова група категорія злочину скільки людей в цій віковій групі зробили цей типу злочину й відсоток відносно своєї вікової групи

location <- suspects |> 
  filter(BORO_NM != "(null)") |> 
  group_by(BORO_NM) |> 
  summarise(total_cases = n()) |> 
  arrange(desc(total_cases)) |> #райони й кількість злочинів
  collect()

suspect_gender <- suspects |> 
  filter(SUSP_SEX != "(null)") |> 
  group_by(SUSP_SEX) |> 
  summarise(total = n()) |> 
  mutate(percentage = total / sum(total) * 100) |> #стать підозрюваних й кількість злочинів
  collect()


#по суті аналогічно можна зробити для постраждалого 

requests_location <- requests_data |>
  filter(Borough != "(null)") |>
  compute(prudence = "lavish") |>
  summarise(total_requests = n(), .by = Borough) |>
  arrange(desc(total_requests)) |>
  collect() #райони й кількість запитів 

location_comparison <- location |>
  inner_join(requests_location, by = c("BORO_NM" = "Borough")) |>
  mutate(
    ratio = total_cases / total_requests
  ) |>
  arrange(desc(ratio))

unique_requests <- requests_data |>
  select(`Complaint Type`) |>
  compute(prudence = "lavish") |>
  collect() |>
  distinct() #не знаю можна зробить топ дебільних запитів або статистика скільки дивних запитів можна почути за день


# crime_requests <- requests_data |>
#   select(`Complaint Type`, `Created Date`, Borough) |>
#   compute(prudence = "lavish") |>
#   collect() |> 
#   filter(grepl("crime|assault|theft|robbery|burglary|arrest", `Complaint Type`, ignore.case = TRUE)) |>
#   group_by(`Complaint Type`, Borough) |>
#   summarise(count = n(), .groups = "drop") |>
#   arrange(desc(count)) #не катіт взагалі в complaints немає нічого супер кримінального, максимум що хтось може бачив наркоту

