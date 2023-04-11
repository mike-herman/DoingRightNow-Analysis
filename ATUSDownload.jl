using Downloads, ZipFile, CSV, DataFrames, ShiftedArrays

"""
    download_atus_data(data_file, year)

Download the given ATUS data file. This saves the zip file in a subdirectory called "data".
This also opens one of the files and returns it as a DataFrame object.

# Arguments
- `data_file`: The type of ATUS data you want to download. One of `("resp", "cps", "act")`.
- `year`: The year you want to pull data for. Note: There is no data for 2020.

# Acceptable `data_file` values
Below are the acceptable values for the `data_file` argument.
- `resp`: The ATUS respondent file. This file contains case-specific variables collected in the ATUS. There is one record for each ATUS respondent. Linking fields are `TUCASEID` and `TULINENO` (always 1 b/c this is the main respondent).
- `cps`: The ATUS CPS file. This contains demographic data about the respondent. There is one record per respondent. Linking fields are `TUCASEID` and `TULINENO`.
- `act`: The ATUS activity file. This contains the activity period of interest. There is one record per respondent-activity. Linking fields are `TUCASEID` and `TUACTIVITY_N`.
"""
function download_atus_data(data_file, year)
    year = string(year)
    zip_file_name = "atus$(data_file)-$(year).zip"
    file_url = "https://www.bls.gov/tus/datafiles/$(zip_file_name)"
    
    # Download the file to the data folder.
    if !ispath("data")
        mkdir(joinpath("data"))
    end

    f = Downloads.download(file_url, joinpath("data",zip_file_name))
    
    # Read the .dat (comma-seperated data) file into a DataFrame.
    data_file_name = "atus$(data_file)_$(year).dat"
    z = ZipFile.Reader(f)
    file_in_zip = filter(x->x.name == data_file_name, z.files)[1]
    df = DataFrame(CSV.File(file_in_zip))
    close(z)
        
    return df
end


"""
    clean_resp_data(resp_dataframe)

This cleans the respondent dataframe and is meant to be used on the result of `download_atus_data`.
Note that this returns a view of the original dataframe. So alterations to the data will change that original.
Generally, we shouldn't be altering the data though.

Returns a dataframe with columns needed for joining and the useful columns.

# Arguments
- `resp_dataframe`: The dataframe with the respondent data.
"""
function clean_resp_df(resp_dataframe)
    # Create a label version of TUDIARYDAY
    diary_day_map = Dict(
        1 => "Sunday",
        2 => "Monday",
        3 => "Tuesday",
        4 => "Wednesday",
        5 => "Thursday",
        6 => "Friday",
        7 => "Saturday"
    )
    resp_dataframe.TUDIARYDAY_label = get.(Ref(diary_day_map), resp_dataframe.TUDIARYDAY, missing)

    return resp_dataframe[!, [:TUCASEID, :TULINENO, :TUDIARYDAY, :TUDIARYDAY_label] ]
end


"""
    clean_activity_df(activity_dy)

Takes a DataFrame of the ATUS activity file.
Returns a view on the original data with cleaned attributes added.

Returns a copy of that DataFarme with a subset of columns and adds two additional view columns:
- `start_time_int`: The integer number of minutes from 4AM when the activity started. This is a lag of `TUCUMDUR24` (defaults to for the first activity).
- `stop_time_int`: The integer number of minutes from 4AM when the activity stopped. This is a pointer to `TUCUMDUR24`.
"""
function clean_activity_df(activity_df)
    sort!(activity_df, [:TUCASEID, :TUACTIVITY_N])    # Sort in place. This will change the order of the input data.
    # Add lagging time integer field.
    transform!(groupby(activity_df, :TUCASEID), :TUCUMDUR24 => (t -> ShiftedArrays.lag(t, 1, default = 0)) => :start_time_int)
    # Copy the original time integer field.
    transform!(activity_df, :TUCUMDUR24 => :stop_time_int)    
    return activity_df[!, [:TUCASEID, :TUACTIVITY_N, :TUSTARTTIM, :TUSTOPTIME, :start_time_int, :stop_time_int, :TRTIER2]]
end


function clean_cps_df(cps_dataframe)
    
    # Filter to just ATUS respondents.
    filter!(row -> row.TRATUSR == 1, cps2021)

    # Create :GESTFIPS_label
    STATE_CODE_CSV = joinpath("data","us-state-ansi-fips.csv")
    state_codes = DataFrame(CSV.File(STATE_CODE_CSV))
    rename!(state_codes, " st" => :GESTFIPS)
    rename!(state_codes, " stusps" => :GESTFIPS_label)
    transform!(state_codes, :state_abbr => (x -> lstrip.(x)) => :state_abbr)
    leftjoin!(cps_dataframe, state_codes, on = :GESTFIPS)

    # Create HEFAMINC_label
    HEFAMINC_replacement_dict = Dict(
        1 => "Less than 5,000",
        2 => "5,000 to 7,499",
        3 => "7,500 to 9,999",
        4 => "10,000 to 12,499",
        5 => "12,500 to 14,999",
        6 => "15,000 to 19,999",
        7 => "20,000 to 24,999",
        8 => "25,000 to 29,999",
        9 => "30,000 to 34,999",
        10 => "35,000 to 39,999",
        11 => "40,000 to 49,999",
        12 => "50,000 to 59,999",
        13 => "60,000 to 74,999",
        14 => "75,000 to 99,999",
        15 => "100,000 to 149,999",
        16 => "150,000 and over"
    )
    cps_dataframe.HEFAMINC_label = map(code_int -> HEFAMINC_replacement_dict[code_int], cps_dataframe.HEFAMINC)

    # Create PEMARITL_label, which simplifies the PEMARITL field.
    PEMARITL_replacement_dict = Dict(
        -1 => missing,
        1 => "Married",
        2 => "Married",
        3 => "Not Married",
        4 => "Not Married",
        5 => "Not Married",
        6 => "Not Married"
    )
    cps_dataframe.PEMARITL_label = map(code_int -> PEMARITL_replacement_dict[code_int], cps_dataframe.PEMARITL)
    
    # Label HETENURE.
     cps_dataframe.HETENURE_label = replace(cps_dataframe.HETENURE, 1 => "Own", 2 => "Rent", 3 => "Non-pay")

    # PRTAGE doesn't need cleaning. It should only have values of 15 or higher.

    want_cols = [:TUCASEID, :TULINENO, :GESTFIPS_label, :HEFAMINC_label, :PEMARITL_label, :HETENURE_label, :PRTAGE]
    return (cps_dataframe[!, want_cols])
end


cps2021 = download_atus_data("cps", 2021)
cps = clean_cps_df(cps2021)