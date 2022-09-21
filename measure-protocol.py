import pandas as pd
import numpy as np
import requests
import collections
from ratelimit import limits, sleep_and_retry

# In prod I would store this in azure key vault to hide/obfuscate the info
WEATHER_API_KEY = "eecda87a4e6bb67b0176b7ea27e17f29"

SURVEY_URL = (
    "https://measure-static.s3.amazonaws.com/take-home-project/data-engineer/survey.csv"
)

# I rate limited this to 60 per minute, but you can remove these decorators. They have a batch API but unfortunately it has a cost.
# you can also do this concurrently with multithreading & workers but I assumed that might get me blocked & didn't want to chance it.
@sleep_and_retry
@limits(calls=60, period=60)
def get_local_info(postal_code):
    response = requests.get(
        f"https://api.openweathermap.org/data/2.5/weather?zip={postal_code},us&appid={WEATHER_API_KEY}&units=imperial"
    )
    return response.json()


# I'm not sure if this is just limited to US cities or I would check if the postal code is 5 digits long.
survey_df = pd.read_csv(SURVEY_URL)


# In reality I wouldn't use 1 large function or class. If you use something like databricks where you have notebook pages,
# splitting things up makes it much easier to troubleshoot later on if something breaks or at least thats my current opinion.
def main():
    # we create two dicts to store the city info so we can pull the data as it is for future transforms. We use one to access via city name & one to access via postal_code
    cities_dict = {}
    weather_dict = {}

    # we can filter on just unique postal codes to lower the amount of queries needed
    unique_columns = survey_df["postal_code"].unique()
    for city in unique_columns:
        # drop any responses that contain non valid postal codes & continue
        try:
            # get the city data
            res = get_local_info(city)
            # store it in the dict with the metadata.
            cities_dict[city] = [
                {
                    "temp": res["main"]["temp"],
                    "temp_min": res["main"]["temp_min"],
                    "temp_max": res["main"]["temp_max"],
                    "temp_avg": (res["main"]["temp_min"] + res["main"]["temp_max"]) / 2,
                    "city": res["name"],
                }
            ]
            weather_dict[res["name"]] = [
                {
                    "temp": res["main"]["temp"],
                    "temp_min": res["main"]["temp_min"],
                    "temp_max": res["main"]["temp_max"],
                    "temp_avg": (res["main"]["temp_min"] + res["main"]["temp_max"]) / 2,
                    "city": res["name"],
                    "postal_code": city,
                }
            ]
        # continue past invalid postal_codes, I would most likely store these in a table/db to let the business owner know later on they may need to look at the data in their system.
        except KeyError:
            continue

    for idx, row in survey_df.iterrows():
        # filter on valid cities with valid postal_codes
        if row["postal_code"] in cities_dict:
            # pull in the data from the cities_dict for each row in the survey excel file & add in the temp, min temp, max temp, & city.
            temp = float(cities_dict.get(row["postal_code"], "")[0]["temp"])
            temp_max = float(cities_dict.get(row["postal_code"], "")[0]["temp_max"])
            temp_min = float(cities_dict.get(row["postal_code"], "")[0]["temp_min"])
            avg_temp = float(cities_dict.get(row["postal_code"], "")[0]["temp_avg"])
            city = cities_dict.get(row["postal_code"], "")[0]["city"]

            # locate the rows & create the new columns with the temp & city name info.
            survey_df.loc[idx, "temperature"] = temp
            survey_df.loc[idx, "temp_max"] = temp_max
            survey_df.loc[idx, "temp_min"] = temp_min
            survey_df.loc[idx, "avg_temp"] = avg_temp
            survey_df.loc[idx, "city"] = city

    # remove any blank/invalid cities. Leave blank genders, we need those for later transformations.
    survey_df["city"].replace("", np.nan, inplace=True)
    survey_df.dropna(subset=["city"], inplace=True)

    # create a file with all metadata we need for future calls in case I get blocked by the API.
    survey_df.to_csv("survery_temp_data.csv")

    # select the columns needed
    df_with_medidata = survey_df[
        ["user_id", "gender", "postal_code", "city", "temperature"]
    ]
    df_with_medidata.to_csv("output.csv", index=False)

    # START GENERATE CITIES BY GENDER
    survey_df["num_users"] = ""

    # group users by gender counts & city, this one wasn't exactly clear what was needed based on the text. My interpretation was since theres only 3 columns to add duplicate cities for each gender count.
    cities_by_gender = (
        survey_df.loc[survey_df["gender"] != ""]
        .groupby(
            [
                survey_df["city"],
                survey_df["gender"],
            ]
        )["num_users"]
        .count()
    )
    cities_by_gender.to_csv("cities_by_gender.csv")
    # END GENERATE CITIES BY GENDER

    # START GENERATE CITIES BY GENDER DISTRIBUTION

    # group cities together with their respective people based on gender, save as a list with .agg
    survey_df_grouped = survey_df.groupby(["city"])["gender"].agg(list).reset_index()

    for idx, row in survey_df_grouped.iterrows():
        # get length of the list to get total users & use to calculate the percents
        total_users = (
            int(collections.Counter(row["gender"])["male"])
            + int(collections.Counter(row["gender"])["female"])
            + int(collections.Counter(row["gender"])["non_binary"])
            + int(collections.Counter(row["gender"])[np.nan])
        )
        # calculate the averages of genders, use collections.Counter to get the count of each gender
        survey_df_grouped.loc[idx, "male_percent"] = (
            collections.Counter(row["gender"])["male"] * 100
        ) / total_users
        survey_df_grouped.loc[idx, "female_percent"] = (
            collections.Counter(row["gender"])["female"] * 100
        ) / total_users
        survey_df_grouped.loc[idx, "non_binary_percent"] = (
            collections.Counter(row["gender"])["non_binary"] * 100
        ) / total_users
        survey_df_grouped.loc[idx, "blank_percent"] = (
            collections.Counter(row["gender"])[np.nan] * 100
        ) / total_users

    survey_df_grouped_by_gender = survey_df_grouped.drop(["gender"], axis=1)

    survey_df_grouped_by_gender.to_csv("cities_by_gender_distribution.csv", index=False)
    # END GENERATE CITIES BY GENDER DISTRIBUTION

    # START GENERATE CITIES BY AVG TEMP
    df_city_temp_avg = survey_df[["city", "avg_temp"]].drop_duplicates()
    df_city_temp_avg.to_csv("cities_by_avg_temp.csv", index=False)
    # ENG GENERATE CITIES BY AVG TEMP

    # START GENERATE TOP 10 CITIES BY TEMP
    # we use .copy to avoid settingwithcopy warnings from pandas
    female_majority_df = survey_df_grouped.loc[
        (survey_df_grouped["female_percent"] >= 50)
    ].copy()

    for idx, row in female_majority_df.iterrows():
        female_majority_df.loc[idx, "avg_temp"] = float(
            weather_dict.get(row["city"], "")[0]["temp_avg"]
        )

    top_ten_female_majority = female_majority_df.nlargest(10, "avg_temp", keep="all")

    top_ten_female_majority = top_ten_female_majority[
        ["city", "avg_temp", "female_percent"]
    ]

    top_ten_female_majority.to_csv("top_10_cities_by_temp.csv", index=False)
    # END GENERATE TOP 10 CITIES BY TEMP


main()
