using Downloads, ZipFile, CSV, DataFrames, ShiftedArrays, Dates, Random, Arrow

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

    if isfile(joinpath("data",zip_file_name))
        f = joinpath("data",zip_file_name)
    else
        f = Downloads.download(file_url, joinpath("data",zip_file_name))
    end

    # Read the .dat (comma-seperated data) file into a DataFrame.
    data_file_name = "atus$(data_file)_$(year).dat"
    z = ZipFile.Reader(f)
    file_in_zip = filter( x -> occursin(data_file_name, x.name), z.files)[1]
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

    return resp_dataframe[!, [:TUCASEID, :TUDIARYDAY, :TUDIARYDAY_label] ]
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


"""
    clean_cps_df(cps_dataframe)

Takes a DataFrame of the ATUS cps file.
Returns a view on the original data with cleaned attributes added.
"""
function clean_cps_df(cps_dataframe)
    
    # Filter to just ATUS respondents.
    filter!(row -> row.TRATUSR == 1, cps_dataframe)

    # Create :GESTFIPS_label
    STATE_CODE_CSV = joinpath("data","us-state-ansi-fips.csv")
    state_codes = DataFrame(CSV.File(STATE_CODE_CSV))
    rename!(state_codes, " st" => :GESTFIPS)
    rename!(state_codes, " stusps" => :GESTFIPS_label)
    transform!(state_codes, :GESTFIPS_label => (x -> lstrip.(x)) => :GESTFIPS_label)
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


"""
    time_to_atus_int(t::Dates.Time)::Int

Converts a timestamp to the integer time in minutes since 4AM.

If a value before 4AM is provided (e.g. 3AM) then the minutes between 4AM and that time the following day is provided.
"""
function time_to_atus_int(t::Dates.Time)::Int
    start_of_day = Dates.Time("00:00:00")
    shifted_t = t - Dates.Hour(4)
    return floor(shifted_t - start_of_day, Dates.Minute).value
    #return Dates.Minute(shifted_t - start_of_day).value
    #return convert(Dates.Minute, t - start_of_day).value
end


"""
    snapshot_filter(snap_t::Union{Int,Dates.Time}, df::DataFrame)::DataFrame

Filter an ATUS activity DataFrame to the activities that occurred during the snapshot time.

# Arguments
- `snap_t`: Either a Time object or an integer representing the number of minutes since 4AM.
- `df`: A DataFrame with the ATUS activity data. Must include `:start_time_int` and `:stop_time_int` columns.

"""
function snapshot_filter(snap_t::Union{Int,Dates.Time}, df::DataFrame; add_snap_time_col=true)::DataFrame
    snap_t_int = isa(snap_t, Dates.Time) ? time_to_atus_int(snap_t) : snap_t

    out_df = filter([:start_time_int, :stop_time_int] => (start, stop) -> start <= snap_t_int < stop, df)

    if add_snap_time_col
        out_df.snap_time_int .= snap_t
    end

    return out_df
end


"""
    generate_snapshots(df::DataFrame, snap_vector::Vector{Int})::DataFrame

Create a snapshot dataframe from ATUS activity file.

This function takes an ATUS activity file and a grid vector of times (as integers).

The function returns a DataFrame with one row per activity that occurred at each time.
The `snap_time_int` column is also appended, allowing us to see which snapshot time was used for each row.
"""
function generate_snapshots(df::DataFrame, snap_vector::Vector{Int})::DataFrame
    # For each element in the snap_vector, take snapshots. This produces a vector of dataframes.
    # reduce this vector to a single dataframe using reduce(vcat,...).
    # Return the result. This will be a copy of the data, not a view.
    out_df = reduce(vcat, snapshot_filter.(snap_vector, Ref(df)))
    return out_df
end


"""
    join_snapshot_data(act_snapshots_df::DataFrame, cps_df::DataFrame, resp_df::DataFrame)::DataFrame

This function joins on atus activity snapshot df, a cps df, and a respondent df on the `:TUCASEID` field.
"""
function join_snapshot_data(act_snapshots_df::DataFrame, cps_df::DataFrame, resp_df::DataFrame)::DataFrame
    out_df = copy(act_snapshots_df)
    leftjoin!(out_df, cps_df, on=:TUCASEID)
    leftjoin!(out_df, resp_df, on=:TUCASEID)
    return out_df
end

"""
    create_data_df(year::Union{Int,String}; snapshot_vector::Vector{Int} = [i for i in 0:5:(1435)], save_df_to_file::Union{Bool, String}=false, return_df::Bool=true)::DataFrame

This function pulls, processes, and returns the all ATUS data used for modelling. It has some arguments for specifying which data and what to do with it.

In particular, the `save_df_to_file` and `return_df` arguments let you specify whether this function saves data to a file, returns the dataframe, or both.

If saving to a file, please use the `.arrow` extension.

# Arguments
- `year::Union{Int,String}`: The year of ATUS data that we're pulling. Can be integer or string.
- `snapshot_vector::Vector{Int}`: (default is [0, 5, ..., 1435]). A vector of snapshots times (as integers) to take. (See the `time_to_atus_int` for converting Time objects to the appropriate integer.)
- `save_df_to_file::Union{Bool,String}`: (default = false). If a filename is provided as a string, this will save the dataframe as an Apache Arrow file in the "data" subdirectory of the current directory.
- `return_df::Bool`: (default = true). If `true`, the function will return the final dataframe.
"""
function create_data_df(year::Union{Int,String}; 
    snapshot_vector = [i for i in 0:5:(1435)],
    save_df_to_file::Union{Bool, String}=false, 
    return_df::Bool=true
    )
    
    # Download the data files and save as dataframe.
    act = download_atus_data("act", year)
    cps = download_atus_data("cps", year)
    resp = download_atus_data("resp", year)

    # Clean the data.
    act = clean_activity_df(act)
    cps = clean_cps_df(cps)
    resp = clean_resp_df(resp)
    
    # Generate activity snapshots.
    act_snaps = generate_snapshots(act, snapshot_vector)

    # Join the datat together.
    out_df = join_snapshot_data(act_snaps, cps, resp)

    if isa(save_df_to_file, String)
        Arrow.write(joinpath("data",save_df_to_file), out_df)
    end

    if return_df
        return out_df
    end
end


