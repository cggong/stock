Red []

do %RedCSV/source/csv-reader.red

get-ticker-list: func [] [
    ticker-list: read http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download
]

to-scheme-value: func [value /local ret] [
    ret: copy ""
    if map? value [
        append ret "((lambda () (define tbl (make-eqv-hashtable))^/"
        foreach [k v] value [
            if not set-word? k [
                make error! append "map key is not string: " k
            ]
            append ret rejoin [" (hashtable-set! tbl " to-scheme-value k " " to-scheme-value v ")^/"]
        ]
        append ret "tbl ))^/"
        return ret
    ]
    if string? value [
        parse value [
            (append ret {"})
            any [{"} (append ret {\"})
                | copy nonquote to [{"} | end] (append ret nonquote)
            ]
            (append ret {"^/})
        ]
        return ret
    ]
    if set-word? value [
        return to-scheme-value to-string value
    ]
    if block? value [
        append ret "(list ^/"
        foreach v value [
            append ret rejoin [to-scheme-value v "^/"]
        ]
        append ret ")^/"
        return ret
    ]
]

define-scheme-value: func [name value] [
    return rejoin ["(define " name "^/" to-scheme-value value "^/)"]
]

if not empty? system/options/args [
    reduce load %backtest-help-prog.red
]
