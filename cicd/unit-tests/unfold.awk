#
# Unfold lines if they are folded with last-line \
#

/.*\\$/ {
    gsub(/\\$/, "");
    multiline = multiline $0
    next
}

// {
    if (multiline) {
        multiline = multiline $0
        print multiline
        multiline = ""
        next
    }
    print
}
