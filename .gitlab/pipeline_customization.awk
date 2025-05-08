#
# This is pipeline customization DSL parser.
#
# Language description:
# Input is text file
# Output:
#   - by default: json with object
#   - value with default_value: awk -f ./pipeline_customization.awk -v query=some_variable -v default_value=default_value
#
#
# Program starts with any line that contains:
# (begin: pipeline_customization)
#
# Program ends with line that contains (end: pipeline_customization).
# Program without end is not program.
# If more than one program in file then only last will be processed
#
# Between program begin and end there are lines.
#
# Checkboxes:
#   [ ] Any text (checkbox: <var>) => "<var>": false is added to json
#   [x] Any text (checkbox: <var>) => "<var>": true is added to json
#
# To be continued...
#
#

BEGIN {
    started = 0;
    ended = 0;
    # current_data
    # valid_data
}

/.*[(]begin: pipeline_customization[)].*/ {
    started = 1;
}

/.*\[ \].*[(]checkbox: *[a-zA-Z_].* *[)].*/ {
    pattern = $0;
    var = gensub(/.*\[ \].*[(]checkbox: *([a-zA-Z_].*) *[)].*/, "\\1", "g", pattern);
    current_data[var] = "false";
}


/.*\[x\].*[(]checkbox: *[a-zA-Z_].* *[)].*/ {
    pattern = $0;
    var = gensub(/.*\[x\].*[(]checkbox: *([a-zA-Z_].*) *[)].*/, "\\1", "g", pattern);
    current_data[var] = "true";
}

/.*[(]end: pipeline_customization[)].*/ {
    if (started) {
        delete valid_data;
        for (k in current_data) {
            valid_data[k] = current_data[k];
        }
        ended = 1;
    }
}

// {
    next;
}

END {
    if (ended) {
        if (query) {
            if (query in valid_data) {
                print valid_data[query];
            } else {
                print default_value;
            }
        } else {
            print "{";
            first = 1;
            for (name in valid_data) {
                print (first ? "   " : "  ,") "\"" name "\": " valid_data[name]
                first = 0;
            }
            print "}"
        }
    } else if (query) {
        print default_value
    }
}

# This self test (begin: pipeline_customization)
#
# this awk script is valid program of pipeline_customization DSL
#
# You can run it with:
# > awk -f ./pipeline_customization.awk ./pipeline_customization.awk | jq
#
# [x] Set one variable to true (checkbox: true_variable)
# [ ] Set anoher variable to false (checkbox: false_variable)
#
# (end: pipeline_customization)
