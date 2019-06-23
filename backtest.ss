(define (system-get-stdout cmd)
    (let-values [(
        (stdin stdout stderr pid) 
        (open-process-ports cmd 'block (make-transcoder (utf-8-codec))))]
        (get-string-all stdout)))

(define (http-get url)
    (system-get-stdout (string-append "curl " url)))

(define (http-get-ticker-list-csv)
    ; https://stackoverflow.com/questions/25338608/download-all-stock-symbol-list-of-a-market
    (http-get "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"))

(define (http-get-ticker-time-series-daily ticker)
    (http-get (string-append "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=" ticker "&outputsize=full&apikey=BHEGI807RTCRMUJV&datatype=csv")))

(define (red-help prog)
    ; write to write-help-prog.red
    (call-with-port
        (open-file-output-port 
            "backtest-help-prog.red" 
            (file-options replace)
            (buffer-mode block)
            (make-transcoder (utf-8-codec)))
        (lambda (p) (put-string p prog)))
    (system-get-stdout (string-append "./red backtest-help.red x")))

(define (red-http-get-parse-csv url)
    (let ([sip (open-string-input-port (red-help (string-append "print to-scheme-value csv/map read to-url \"" url "\"")))])
        (eval (get-datum sip) (scheme-environment))))

(define (http-get-ticker-list)
    ; https://stackoverflow.com/questions/25338608/download-all-stock-symbol-list-of-a-market
    (red-http-get-parse-csv "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"))

(define attributes
    '((start-date ticker)
      (end-date ticker)
      (date ticker day)
      (close-price ticker day)
      (volume ticker day)))
(define num-raw-attributes 4)

; (module stockdata
;     (define tbl (make-eqv-hashtable))
;     (for-each
;         (lambda (attribute)
;             (hashtable-set! tbl attribute (make-eqv-hashtable)))
;         (map car attributes))
;     )

; (module backtest (indicators last-close close-at close-at-relative
;                     next-open next-close)
;     (define indicators (make-indicators)))

; (define (mov-avg)
;     (import backtest)
;     (indicators-mov-avg-set! indicators
;         (do [(i 0 (+ i 1)) (s 0 (+ s last-close))]
;             ((= i 4) (/ s 5))
;             (next-close)))
;     (do []
;         (#f)
;         (next-close)
;         (indicators-mov-avg-set! indicators
;             (+ (indicators-mov-avg indicators)
;                 (/ (- last-close (close-at-relative 5)))))))


; (display (strategy #f))

; TODO: 
; Extend mov-avg to support stock ticker.
; Indicators should be able to take some arguments, e.g. stock ticker, window size for mov-avg. 
; Different indicators take different argument list. 
; (backtest indicators)
; (backtest last-close)
; (backtest close-at)
; (backtest close-at-relative)
; (backtest next-open)
; (backtest next-close)

; Done:
; Add shortcut for (backtest indicators) as (indicators), etc. June 20