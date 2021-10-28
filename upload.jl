using CSV
using TypeDBClient: dbconnect, open, read, write, match, insert, commit, create_database, delete_database
using Formatting

## Create unit database
# dbconnect("127.0.0.1", 1729) do client
#     create_database(client, "units")
# end

## Delete unit database
# dbconnect("127.0.0.1", 1729) do client
#     delete_database(client, "units")
# end


## Formatting Functions
function format_dims_insert(d)
    """
    insert \$dim isa dimension_type; \$dim "$(d.dimension_type)"; 
    \$dim has dim_t $(d.dim_t), has dim_l $(d.dim_l), 
    has dim_m $(d.dim_m), has dim_i $(d.dim_i), has dim_k $(d.dim_k),  
    has dim_j $(d.dim_j),  has dim_r $(d.dim_r);
    """
end

function format_prefix_insert(p)
    """
    insert \$prefix isa prefix; \$prefix "$(p.prefix)"; 
    \$prefix has prefix_name "$(p.prefix_name)",
    has power_ten $(p.power_ten);
    """
end

precision(x) = ceil(Int, -log10(x))

function format_unit_insert(u)
    # TODO: handle missing data no matter where it is
    if ismissing(u.prefix)
        prefix = ""
    else
        prefix = u.prefix
    end

    # Bodge to fix number formatting
    c_ratio = u.c_ratio
    if u.c_ratio <= 0.0001
        c_ratio = format(u.c_ratio, precision=precision(u.c_ratio))
    end

    query = """
    \$base has uname "$(u.uname)",  
    has c_ratio $(c_ratio), 
    has prefix "$(prefix)", 
    has isCore $(u.isCore),
    has latex_string "$(u.latex_string)",
    has dimension_type "$(u.dimension_type)" """
    
    # Subtype unit to affine unit it it has a non-zero logbase
    if u.c_logbase != 0
        query *= ",\nhas c_logbase $(u.c_logbase);"
        return """insert \$base isa log_unit; \$base "$(u.unit)";""" * query
    end

    # Subtype unit to affine unit it it has a non-zero offset
    if u.c_offset != 0
        query *= ",\nhas c_offset $(u.c_offset);"
        return """insert \$base isa affine_unit; \$base "$(u.unit)";""" * query
    end

    # A normal unit (not log or affine)
    return """insert \$base isa unit; \$base "$(u.unit)";""" * query * ";"
end


function format_example_insert(e)
    """
    insert \$measure isa measure; \$measure $(e.measure); 
    \$measure has unit "$(e.unit)";
    """
end

function format_corescalerelation_insert(r)
    """
    match
        \$unit isa unit; \$unit "$(r.unit)";
        \$core  isa unit; \$core "$(r.core)"; \$core has isCore true;
    insert \$core_conversion (core: \$core, scaled: \$unit) isa core_conversion;
    """
end


# Insert Data Helper function
function insert_data(session, data, formatter::Function)
    for row in data
        write(session) do transaction
            insert(transaction, formatter(row))
            commit(transaction)
        end 
    end
end

## Load data
dimsdata = CSV.File("data/dimensions.csv");
prefdata = CSV.File("data/si_prefixes.csv");
coredata = CSV.File("data/core.csv");
scaldata = CSV.File("data/scaled.csv");
expldata = CSV.File("data/examples.csv");

dbconnect("127.0.0.1", 1729) do client
    # Open the session
    open(client, "units") do session
        # Insert dimensions and prefixes
        insert_data(session, dimsdata, format_dims_insert)
        insert_data(session, prefdata, format_prefix_insert)

        # Insert different unit types
        insert_data(session, coredata, format_unit_insert)
        insert_data(session, scaldata, format_unit_insert)

        # Insert example data
        insert_data(session, expldata, format_example_insert)
    end
end

@info "Complete"

## Insert relations
dbconnect("127.0.0.1", 1729) do client
    # Open the session
    open(client, "units") do session
        insert_data(session, scaldata, format_corescalerelation_insert)
    end
end
@info "Complete"


## Printing Queries for Debugging
dimsdata .|> format_dims_insert .|> println;
prefdata .|> format_prefix_insert .|> println;
coredata .|> format_unit_insert .|> println;
scaldata .|> format_unit_insert .|> println;
expldata .|> format_example_insert .|> println;

scaldata .|> format_corescalerelation_insert .|> println;

