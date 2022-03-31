PARTNER_BY_PHONE_QUERY = """
    exec sp_executesql N'SELECT
        T3._IDRRef AS ref,
        T3._Description AS name,
        T3._Fld52315 AS full_name,
        T3._Code AS code
    FROM dbo._Reference243_VT49912 T1
        LEFT OUTER JOIN dbo._Reference243 T2
        ON (T1._Reference243_IDRRef = T2._IDRRef) AND (T2._Fld2150 = @P1)
        LEFT OUTER JOIN dbo._Reference347 T3
        ON (T2._OwnerIDRRef = T3._IDRRef) AND (T3._Fld2150 = @P2)
        LEFT OUTER JOIN dbo._Reference347 T4
        ON (T2._OwnerIDRRef = T4._IDRRef) AND (T4._Fld2150 = @P3)
    WHERE (T1._Fld2150 = @P4) AND ((T2._Marked = 0x00) AND T1._Fld49915RRef IN
        (SELECT
        T5._IDRRef AS Q_001_F_000RRef
        FROM dbo._Reference81 T5
        WHERE 
            (T5._Fld2150 = @P5) 
            AND ((T5._Marked = 0x00) 
            AND (T5._Fld88335 LIKE @P6))) 
            AND (T1._Fld49923 LIKE @P7) 
            AND (T4._Marked = 0x00))',
    N'@P1 numeric(10),
    @P2 numeric(10),
    @P3 numeric(10),
    @P4 numeric(10),
    @P5 numeric(10),
    @P6 nvarchar(4000),
    @P7 nvarchar(4000)',
    0,
    0,
    0,
    0,
    0,
    N'МобильныйТелефонКонтактногоЛица',
    ?
    --N'7910081502'					
"""

REPORT_QUERY = """
        exec sp_executesql N'SELECT
        T16._Fld52315 AS Partner_Name,
        T17._Code AS Wagon,
        DATEADD(YEAR, -2000, T8.Q_001_F_001_) AS Operation_Date,
        T18._Description AS Operation_Station_Name,
        T19._Description AS Operation_Railway_Name,
        T20._Description  AS Operation_Name,
        ((T21._Code + N'' '') + T21._Description) AS Cargo_Name,
        T22._Description AS Departure_Station_Name,
        T23._Description AS Departure_Railway_Name,
        T24._Description AS Destination_Station_Name,
        T25._Description AS Destination_Railway_Name,
        CASE
            WHEN YEAR(T8.Q_001_F_007_) = 2001 THEN NULL
            ELSE DATEADD(YEAR, -2000, T8.Q_001_F_007_)
        END AS Delivery_Date,
        T8.Q_001_F_008_ AS Cargo_Weight,
        T8.Q_001_F_009_ AS Train_Number,
        T8.Q_001_F_010_ AS Train_Index,
        T8.Q_001_F_011_ AS Cargo_Sender_OKPO,
        T8.Q_001_F_012_ AS Cargo_Receiver_OKPO,
        T8.Q_001_F_013_ AS Parking_Time,
        T8.Q_001_F_014_ AS Days_Until_Repair,
        CONVERT(DATE, DATEADD(YEAR, -2000, T8.Q_001_F_015_)) AS Next_SR_Date,
        T8.Q_001_F_016_ AS Capacity,
        T26._Description AS Next_Load_Station_Name,
        T8.Q_001_F_017_ AS Owner,
        CASE 
            WHEN (T1.Q_001_F_003RRef = 0x8E0DE1104ED296014CB4844C8CE5258A) THEN N''      1. На подходе под погрузку'' 
            WHEN (T1.Q_001_F_003RRef = 0x993F47B874AFD0C240F9F19C709F85DA) THEN N''      2. На станции погрузки'' 
            WHEN (T1.Q_001_F_003RRef = 0x980F35AE8BAA9D6C450584F69C7B5BD5) THEN N''      3. В пути гружёные'' 
            WHEN (T1.Q_001_F_003RRef = 0xA004378A0531A7774D214ED7CBCC4B25) THEN N''      4. На выгрузке'' 
            ELSE N''      5. Статус требует уточнения'' 
        END AS Feature,
        T8.Q_001_F_018_ AS Remaining_Distance,
        CASE YEAR(
            DATEADD(YEAR, -2000,
                CASE 
                    WHEN (T1.Q_001_F_003RRef = 0x980F35AE8BAA9D6C450584F69C7B5BD5) 
                        OR (T1.Q_001_F_003RRef = 0x993F47B874AFD0C240F9F19C709F85DA) 
                        OR (T1.Q_001_F_003RRef = 0x8240F232234216EC4CDA66C986A1F079) 
                        OR (T1.Q_001_F_003RRef = 0xA004378A0531A7774D214ED7CBCC4B25) THEN T8.Q_001_F_019_ 
                    ELSE {ts ''4001-01-01 00:00:00''}
                END))
            WHEN 2001 THEN NULL
            WHEN 1 THEN NULL
            ELSE
                DATEADD(YEAR, -2000,
                    CASE 
                        WHEN (T1.Q_001_F_003RRef = 0x980F35AE8BAA9D6C450584F69C7B5BD5) 
                            OR (T1.Q_001_F_003RRef = 0x993F47B874AFD0C240F9F19C709F85DA) 
                            OR (T1.Q_001_F_003RRef = 0x8240F232234216EC4CDA66C986A1F079) 
                            OR (T1.Q_001_F_003RRef = 0xA004378A0531A7774D214ED7CBCC4B25) THEN T8.Q_001_F_019_ 
                        ELSE {ts ''4001-01-01 00:00:00''}
                    END)
        END AS Load_Date
        FROM (SELECT
        T2.Fld74248RRef AS Q_001_F_000RRef,
        T7._Fld49943RRef AS Q_001_F_001RRef,
        T2.Fld74315RRef AS Q_001_F_002RRef,
        T2.Fld74372RRef AS Q_001_F_003RRef
        FROM (SELECT
        T5._Fld74372RRef AS Fld74372RRef,
        T5._Fld74248RRef AS Fld74248RRef,
        T5._Fld74249RRef AS Fld74249RRef,
        T5._Fld74315RRef AS Fld74315RRef
        FROM (SELECT
        T4._Fld74248RRef AS Fld74248RRef,
        T4._Fld74249RRef AS Fld74249RRef,
        T4._Fld74250 AS Fld74250_,
        MAX(T4._Period) AS MAXPERIOD_
        FROM dbo._InfoRg74247 T4
        WHERE ((T4._Fld2150 = @P1)) AND (T4._Active = 0x01)
        GROUP BY T4._Fld74248RRef,
        T4._Fld74249RRef,
        T4._Fld74250) T3
        INNER JOIN dbo._InfoRg74247 T5
        ON 
            T3.Fld74248RRef = T5._Fld74248RRef 
            AND T3.Fld74249RRef = T5._Fld74249RRef 
            AND T3.Fld74250_ = T5._Fld74250 
            AND T3.MAXPERIOD_ = T5._Period
        WHERE (T5._Fld2150 = @P2)) T2
        LEFT OUTER JOIN dbo._Document74137 T6
        ON (T2.Fld74249RRef = T6._IDRRef) AND (T6._Fld2150 = @P3)
        LEFT OUTER JOIN dbo._Reference244 T7
        ON (T6._Fld74165RRef = T7._IDRRef) AND (T7._Fld2150 = @P4)
        WHERE (NOT (((T2.Fld74372RRef = @P5) OR (T2.Fld74372RRef = @P6) OR (T2.Fld74372RRef = @P7))))) T1
        LEFT OUTER JOIN (SELECT
        T9._Fld74101RRef AS Q_001_F_000RRef,
        T9._Fld74102 AS Q_001_F_001_,
        T9._Fld74103RRef AS Q_001_F_002RRef,
        T9._Fld74130RRef AS Q_001_F_003RRef,
        T9._Fld74109RRef AS Q_001_F_004RRef,
        T9._Fld74104RRef AS Q_001_F_005RRef,
        T9._Fld74105RRef AS Q_001_F_006RRef,
        T9._Fld74114 AS Q_001_F_007_,
        T9._Fld74119 AS Q_001_F_008_,
        T9._Fld74107 AS Q_001_F_009_,
        T9._Fld74106 AS Q_001_F_010_,
        T9._Fld74110 AS Q_001_F_011_,
        T9._Fld74111 AS Q_001_F_012_,
        T9._Fld74117 AS Q_001_F_013_,
        CAST(DATEDIFF(DAY,@P8,T10.Fld74291_) AS NUMERIC(12)) AS Q_001_F_014_,
        T10.Fld74291_ AS Q_001_F_015_,
        T14._Fld74004 AS Q_001_F_016_,
        T14._Fld92065 AS Q_001_F_017_,
        T9._Fld74115 AS Q_001_F_018_,
        T9._Fld74112 AS Q_001_F_019_
        FROM dbo._InfoRg74100 T9
        LEFT OUTER JOIN (SELECT
        T13._Fld74289RRef AS Fld74289RRef,
        T13._Fld74291 AS Fld74291_
        FROM (SELECT
        T12._Fld74289RRef AS Fld74289RRef,
        MAX(T12._Period) AS MAXPERIOD_
        FROM dbo._InfoRg74288 T12
        WHERE ((T12._Fld2150 = @P9)) AND (T12._Period <= @P10)
        GROUP BY T12._Fld74289RRef) T11
        INNER JOIN dbo._InfoRg74288 T13
        ON T11.Fld74289RRef = T13._Fld74289RRef AND T11.MAXPERIOD_ = T13._Period
        WHERE (T13._Fld2150 = @P11)) T10
        ON (T9._Fld74101RRef = T10.Fld74289RRef)
        LEFT OUTER JOIN dbo._Reference72894 T14
        ON ((T9._Fld74101RRef = T14._IDRRef)) AND (T14._Fld2150 = @P12)
        WHERE ((T9._Fld2150 = @P13)) AND (EXISTS(SELECT
        1
        FROM dbo._InfoRg74100 T15
        WHERE (T15._Fld2150 = @P14)
        GROUP BY T15._Fld74101RRef
        HAVING (T9._Fld74101RRef = T15._Fld74101RRef) AND (T9._Fld74102 = MAX(T15._Fld74102))))) T8
        ON (T1.Q_001_F_000RRef = T8.Q_001_F_000RRef)
        LEFT OUTER JOIN dbo._Reference347 T16
        ON (T1.Q_001_F_001RRef = T16._IDRRef) AND (T16._Fld2150 = @P15)
        LEFT OUTER JOIN dbo._Reference72894 T17
        ON (T1.Q_001_F_000RRef = T17._IDRRef) AND (T17._Fld2150 = @P16)
        LEFT OUTER JOIN dbo._Reference72895 T18
        ON (T8.Q_001_F_002RRef = T18._IDRRef) AND (T18._Fld2150 = @P17)
        LEFT OUTER JOIN dbo._Reference72896 T19
        ON (T18._Fld72899RRef = T19._IDRRef) AND (T19._Fld2150 = @P18)
        LEFT OUTER JOIN dbo._Reference74125 T20
        ON (T8.Q_001_F_003RRef = T20._IDRRef) AND (T20._Fld2150 = @P19)
        LEFT OUTER JOIN dbo._Reference72892 T21
        ON (T8.Q_001_F_004RRef = T21._IDRRef) AND (T21._Fld2150 = @P20)
        LEFT OUTER JOIN dbo._Reference72895 T22
        ON (T8.Q_001_F_005RRef = T22._IDRRef) AND (T22._Fld2150 = @P21)
        LEFT OUTER JOIN dbo._Reference72896 T23
        ON (T22._Fld72899RRef = T23._IDRRef) AND (T23._Fld2150 = @P22)
        LEFT OUTER JOIN dbo._Reference72895 T24
        ON (T8.Q_001_F_006RRef = T24._IDRRef) AND (T24._Fld2150 = @P23)
        LEFT OUTER JOIN dbo._Reference72896 T25
        ON (T24._Fld72899RRef = T25._IDRRef) AND (T25._Fld2150 = @P24)
        LEFT OUTER JOIN dbo._Reference72895 T26
        ON (T1.Q_001_F_002RRef = T26._IDRRef) AND (T26._Fld2150 = @P25)
        WHERE (T1.Q_001_F_001RRef = @P26)
        ORDER BY 
            (CASE 
                WHEN (T1.Q_001_F_003RRef = 0x8E0DE1104ED296014CB4844C8CE5258A) 
                    THEN N''      1. На подходе под погрузку'' 
                WHEN (T1.Q_001_F_003RRef = 0x993F47B874AFD0C240F9F19C709F85DA) 
                    THEN N''      2. На станции погрузки'' 
                WHEN (T1.Q_001_F_003RRef = 0x980F35AE8BAA9D6C450584F69C7B5BD5) 
                    THEN N''      3. В пути гружёные'' 
                WHEN (T1.Q_001_F_003RRef = 0xA004378A0531A7774D214ED7CBCC4B25) 
                    THEN N''      4. На выгрузке'' 
                ELSE 
                    N''      5. Статус требует уточнения'' 
            END), 
            (T17._Code)',
        N'@P1 numeric(10),
        @P2 numeric(10),
        @P3 numeric(10),
        @P4 numeric(10),
        @P5 varbinary(16),
        @P6 varbinary(16),
        @P7 varbinary(16),
        @P8 datetime2(3),
        @P9 numeric(10),
        @P10 datetime2(3),
        @P11 numeric(10),
        @P12 numeric(10),
        @P13 numeric(10),
        @P14 numeric(10),
        @P15 numeric(10),
        @P16 numeric(10),
        @P17 numeric(10),
        @P18 numeric(10),
        @P19 numeric(10),
        @P20 numeric(10),
        @P21 numeric(10),
        @P22 numeric(10),
        @P23 numeric(10),
        @P24 numeric(10),
        @P25 numeric(10),
        @P26 varbinary(16)',
        0,
        0,
        0,
        0,
        0xB961F2E7EAA91C9C4D1D33D273690F56,
        0x9B8E8C1C7B0837D14B91DD88666891A6,
        0x9832C74C05E61CC548D42240A533A90C,
        --'4021-07-15 00:00:00',
        ?,
        0,
        --'4021-07-15 00:00:00',
        ?,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        --0x814000155D0A023E11E9295608D4E1B5
        ?					
    """

REPORT_QUERY2 = """
        exec sp_executesql N'SELECT
        T16._Fld52315 AS Partner_Name,
        T17._Code AS Wagon,
        DATEADD(YEAR, -2000, T8.Q_001_F_001_) AS Operation_Date,
        T18._Description AS Operation_Station_Name,
        T19._Description AS Operation_Railway_Name,
        T20._Description  AS Operation_Name,
        ((T21._Code + N'' '') + T21._Description) AS Cargo_Name,
        T22._Description AS Departure_Station_Name,
        T23._Description AS Departure_Railway_Name,
        T24._Description AS Destination_Station_Name,
        T25._Description AS Destination_Railway_Name,
        CASE
            WHEN YEAR(T8.Q_001_F_007_) = 2001 THEN NULL
            ELSE DATEADD(YEAR, -2000, T8.Q_001_F_007_)
        END AS Delivery_Date,
        T8.Q_001_F_008_ AS Cargo_Weight,
        T8.Q_001_F_009_ AS Train_Number,
        T8.Q_001_F_010_ AS Train_Index,
        T8.Q_001_F_011_ AS Cargo_Sender_OKPO,
        T8.Q_001_F_012_ AS Cargo_Receiver_OKPO,
        T8.Q_001_F_013_ AS Parking_Time,
        T8.Q_001_F_014_ AS Days_Until_Repair,
        CONVERT(DATE, DATEADD(YEAR, -2000, T8.Q_001_F_015_)) AS Next_SR_Date,
        T8.Q_001_F_016_ AS Capacity,
        T26._Description AS Next_Load_Station_Name,
        T8.Q_001_F_017_ AS Owner,
        CASE 
            WHEN (T1.Q_001_F_003RRef = 0x8E0DE1104ED296014CB4844C8CE5258A) THEN N''      1. На подходе под погрузку'' 
            WHEN (T1.Q_001_F_003RRef = 0x993F47B874AFD0C240F9F19C709F85DA) THEN N''      2. На станции погрузки'' 
            WHEN (T1.Q_001_F_003RRef = 0x980F35AE8BAA9D6C450584F69C7B5BD5) THEN N''      3. В пути гружёные'' 
            WHEN (T1.Q_001_F_003RRef = 0xA004378A0531A7774D214ED7CBCC4B25) THEN N''      4. На выгрузке'' 
            ELSE N''      5. Статус требует уточнения'' 
        END AS Feature,
        T8.Q_001_F_018_ AS Remaining_Distance,
        CASE YEAR(
            DATEADD(YEAR, -2000,
                CASE 
                    WHEN (T1.Q_001_F_003RRef = 0x980F35AE8BAA9D6C450584F69C7B5BD5) 
                        OR (T1.Q_001_F_003RRef = 0x993F47B874AFD0C240F9F19C709F85DA) 
                        OR (T1.Q_001_F_003RRef = 0x8240F232234216EC4CDA66C986A1F079) 
                        OR (T1.Q_001_F_003RRef = 0xA004378A0531A7774D214ED7CBCC4B25) THEN T8.Q_001_F_019_ 
                    ELSE {ts ''4001-01-01 00:00:00''}
                END))
            WHEN 2001 THEN NULL
            WHEN 1 THEN NULL
            ELSE
                DATEADD(YEAR, -2000,
                    CASE 
                        WHEN (T1.Q_001_F_003RRef = 0x980F35AE8BAA9D6C450584F69C7B5BD5) 
                            OR (T1.Q_001_F_003RRef = 0x993F47B874AFD0C240F9F19C709F85DA) 
                            OR (T1.Q_001_F_003RRef = 0x8240F232234216EC4CDA66C986A1F079) 
                            OR (T1.Q_001_F_003RRef = 0xA004378A0531A7774D214ED7CBCC4B25) THEN T8.Q_001_F_019_ 
                        ELSE {ts ''4001-01-01 00:00:00''}
                    END)
        END AS Load_Date
        FROM (SELECT
        T2.Fld74248RRef AS Q_001_F_000RRef,
        T7._Fld49943RRef AS Q_001_F_001RRef,
        T2.Fld74315RRef AS Q_001_F_002RRef,
        T2.Fld74372RRef AS Q_001_F_003RRef
        FROM (SELECT
        T5._Fld74372RRef AS Fld74372RRef,
        T5._Fld74248RRef AS Fld74248RRef,
        T5._Fld74249RRef AS Fld74249RRef,
        T5._Fld74315RRef AS Fld74315RRef
        FROM (SELECT
        T4._Fld74248RRef AS Fld74248RRef,
        T4._Fld74249RRef AS Fld74249RRef,
        T4._Fld74250 AS Fld74250_,
        MAX(T4._Period) AS MAXPERIOD_
        FROM dbo._InfoRg74247 T4
        WHERE ((T4._Fld2150 = @P1)) AND (T4._Active = 0x01)
        GROUP BY T4._Fld74248RRef,
        T4._Fld74249RRef,
        T4._Fld74250) T3
        INNER JOIN dbo._InfoRg74247 T5
        ON 
            T3.Fld74248RRef = T5._Fld74248RRef 
            AND T3.Fld74249RRef = T5._Fld74249RRef 
            AND T3.Fld74250_ = T5._Fld74250 
            AND T3.MAXPERIOD_ = T5._Period
        WHERE (T5._Fld2150 = @P2)) T2
        LEFT OUTER JOIN dbo._Document74137 T6
        ON (T2.Fld74249RRef = T6._IDRRef) AND (T6._Fld2150 = @P3)
        LEFT OUTER JOIN dbo._Reference244 T7
        ON (T6._Fld74165RRef = T7._IDRRef) AND (T7._Fld2150 = @P4)
        WHERE (NOT (((T2.Fld74372RRef = @P5) OR (T2.Fld74372RRef = @P6) OR (T2.Fld74372RRef = @P7))))) T1
        LEFT OUTER JOIN (SELECT
        T9._Fld74101RRef AS Q_001_F_000RRef,
        T9._Fld74102 AS Q_001_F_001_,
        T9._Fld74103RRef AS Q_001_F_002RRef,
        T9._Fld74130RRef AS Q_001_F_003RRef,
        T9._Fld74109RRef AS Q_001_F_004RRef,
        T9._Fld74104RRef AS Q_001_F_005RRef,
        T9._Fld74105RRef AS Q_001_F_006RRef,
        T9._Fld74114 AS Q_001_F_007_,
        T9._Fld74119 AS Q_001_F_008_,
        T9._Fld74107 AS Q_001_F_009_,
        T9._Fld74106 AS Q_001_F_010_,
        T9._Fld74110 AS Q_001_F_011_,
        T9._Fld74111 AS Q_001_F_012_,
        T9._Fld74117 AS Q_001_F_013_,
        CAST(DATEDIFF(DAY,@P8,T10.Fld74291_) AS NUMERIC(12)) AS Q_001_F_014_,
        T10.Fld74291_ AS Q_001_F_015_,
        T14._Fld74004 AS Q_001_F_016_,
        T14._Fld92065 AS Q_001_F_017_,
        T9._Fld74115 AS Q_001_F_018_,
        T9._Fld74112 AS Q_001_F_019_
        FROM dbo._InfoRg74100 T9
        LEFT OUTER JOIN (SELECT
        T13._Fld74289RRef AS Fld74289RRef,
        T13._Fld74291 AS Fld74291_
        FROM (SELECT
        T12._Fld74289RRef AS Fld74289RRef,
        MAX(T12._Period) AS MAXPERIOD_
        FROM dbo._InfoRg74288 T12
        WHERE ((T12._Fld2150 = @P9)) AND (T12._Period <= @P10)
        GROUP BY T12._Fld74289RRef) T11
        INNER JOIN dbo._InfoRg74288 T13
        ON T11.Fld74289RRef = T13._Fld74289RRef AND T11.MAXPERIOD_ = T13._Period
        WHERE (T13._Fld2150 = @P11)) T10
        ON (T9._Fld74101RRef = T10.Fld74289RRef)
        LEFT OUTER JOIN dbo._Reference72894 T14
        ON ((T9._Fld74101RRef = T14._IDRRef)) AND (T14._Fld2150 = @P12)
        WHERE ((T9._Fld2150 = @P13)) AND (EXISTS(SELECT
        1
        FROM dbo._InfoRg74100 T15
        WHERE (T15._Fld2150 = @P14)
        GROUP BY T15._Fld74101RRef
        HAVING (T9._Fld74101RRef = T15._Fld74101RRef) AND (T9._Fld74102 = MAX(T15._Fld74102))))) T8
        ON (T1.Q_001_F_000RRef = T8.Q_001_F_000RRef)
        LEFT OUTER JOIN dbo._Reference347 T16
        ON (T1.Q_001_F_001RRef = T16._IDRRef) AND (T16._Fld2150 = @P15)
        LEFT OUTER JOIN dbo._Reference72894 T17
        ON (T1.Q_001_F_000RRef = T17._IDRRef) AND (T17._Fld2150 = @P16)
        LEFT OUTER JOIN dbo._Reference72895 T18
        ON (T8.Q_001_F_002RRef = T18._IDRRef) AND (T18._Fld2150 = @P17)
        LEFT OUTER JOIN dbo._Reference72896 T19
        ON (T18._Fld72899RRef = T19._IDRRef) AND (T19._Fld2150 = @P18)
        LEFT OUTER JOIN dbo._Reference74125 T20
        ON (T8.Q_001_F_003RRef = T20._IDRRef) AND (T20._Fld2150 = @P19)
        LEFT OUTER JOIN dbo._Reference72892 T21
        ON (T8.Q_001_F_004RRef = T21._IDRRef) AND (T21._Fld2150 = @P20)
        LEFT OUTER JOIN dbo._Reference72895 T22
        ON (T8.Q_001_F_005RRef = T22._IDRRef) AND (T22._Fld2150 = @P21)
        LEFT OUTER JOIN dbo._Reference72896 T23
        ON (T22._Fld72899RRef = T23._IDRRef) AND (T23._Fld2150 = @P22)
        LEFT OUTER JOIN dbo._Reference72895 T24
        ON (T8.Q_001_F_006RRef = T24._IDRRef) AND (T24._Fld2150 = @P23)
        LEFT OUTER JOIN dbo._Reference72896 T25
        ON (T24._Fld72899RRef = T25._IDRRef) AND (T25._Fld2150 = @P24)
        LEFT OUTER JOIN dbo._Reference72895 T26
        ON (T1.Q_001_F_002RRef = T26._IDRRef) AND (T26._Fld2150 = @P25)
        WHERE (T1.Q_001_F_001RRef = @P26)
        ORDER BY 
            (CASE 
                WHEN (T1.Q_001_F_003RRef = 0x8E0DE1104ED296014CB4844C8CE5258A) 
                    THEN N''      1. На подходе под погрузку'' 
                WHEN (T1.Q_001_F_003RRef = 0x993F47B874AFD0C240F9F19C709F85DA) 
                    THEN N''      2. На станции погрузки'' 
                WHEN (T1.Q_001_F_003RRef = 0x980F35AE8BAA9D6C450584F69C7B5BD5) 
                    THEN N''      3. В пути гружёные'' 
                WHEN (T1.Q_001_F_003RRef = 0xA004378A0531A7774D214ED7CBCC4B25) 
                    THEN N''      4. На выгрузке'' 
                ELSE 
                    N''      5. Статус требует уточнения'' 
            END), 
            (T17._Code)',
        N'@P1 numeric(10),
        @P2 numeric(10),
        @P3 numeric(10),
        @P4 numeric(10),
        @P5 varbinary(16),
        @P6 varbinary(16),
        @P7 varbinary(16),
        @P8 datetime2(3),
        @P9 numeric(10),
        @P10 datetime2(3),
        @P11 numeric(10),
        @P12 numeric(10),
        @P13 numeric(10),
        @P14 numeric(10),
        @P15 numeric(10),
        @P16 numeric(10),
        @P17 numeric(10),
        @P18 numeric(10),
        @P19 numeric(10),
        @P20 numeric(10),
        @P21 numeric(10),
        @P22 numeric(10),
        @P23 numeric(10),
        @P24 numeric(10),
        @P25 numeric(10),
        @P26 varbinary(16)',
        0,
        0,
        0,
        0,
        0xB961F2E7EAA91C9C4D1D33D273690F56,
        0x9B8E8C1C7B0837D14B91DD88666891A6,
        0x9832C74C05E61CC548D42240A533A90C,
        --'4021-07-15 00:00:00',
        ?,
        0,
        --'4021-07-15 00:00:00',
        ?,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        --0x814000155D0A023E11E9295608D4E1B5
        ?					
    """

CARGO_CODE_BY_SUBSTRING_QUERY = """
    exec sp_executesql N'SELECT
        T1._Code AS Code,
        T1._Description AS Name
    FROM 
        dbo._Reference72892 T1
    WHERE 
        ((T1._Fld2150 = @P1)) 
        AND ((T1._Marked = 0x00) 
        AND ((T1._Code LIKE @P3) OR (T1._Description LIKE @P3)))
    ORDER BY 
        (T1._Description)',
    N'@P1 numeric(10),
    --@P2 nvarchar(4000),
    @P3 nvarchar(4000)',
    0,
    --N'%501%',
    ?					
    
"""

PACKAGE_TYPE_CODE_BY_SUBSTRING_QUERY = """
    exec sp_executesql N'SELECT
        T1._Code AS NumCode,
        T1._Description AS Name
    FROM 
        dbo._Reference92342 T1
    WHERE 
        ((T1._Fld2150 = 0)) AND ((T1._Marked = 0x00))
    ORDER BY 
        (T1._Description)'
"""

STATION_CODE_BY_SUBSTRING_QUERY = """
    exec sp_executesql N'SELECT
        T1._Code AS Code,
        T1._Description AS Name,
        T2._Description AS RW_Name
    FROM dbo._Reference72895 T1
    LEFT OUTER JOIN dbo._Reference72896 T2
        ON (T1._Fld72899RRef = T2._IDRRef) AND (T2._Fld2150 = @P1)
    WHERE 
        ((T1._Fld2150 = @P2)) 
        AND ((T1._Marked = 0x00) 
            AND ((T1._Code LIKE @P3) OR (T1._Description LIKE @P3)))
    ORDER BY 
        (T1._Description), 
        (T2._Description)',
        N'@P1 numeric(10),
        @P2 numeric(10),
        @P3 nvarchar(4000)',
        0,
        0,
        ?
"""

WAGON_TYPE_QUERY = """
    exec sp_executesql N'SELECT
        T1._Code AS NumCode,
        T1._Description AS Name
    FROM dbo._Reference73994 T1
    WHERE 
        ((T1._Fld2150 = @P1)) AND ((T1._Marked = 0x00))
    ORDER BY (T1._Description)',
        N'@P1 numeric(10)',
        0
"""

# CALCULATE_PRICE_XML = """
# <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
# <TPIN_ServiceRequest xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="https://etpgp.rzd.ru/schema/4.0/TPIN_Service.xsd">
#     <freight>
#         <fr_code>%cargo_type_code%</fr_code>
#         <weight>%cargo_weight%</weight>
#         <volume>%cargo_volume%</volume>
#         <packTypeID>%cargo_package_type_code%</packTypeID>
#     </freight>
#     <startDate>%start_date%</startDate>
#     <finishDate>%finish_date%</finishDate>
#     <sndStation>
#         <stCode>%departure_station_code%</stCode>
#     </sndStation>
#     <rsvStation>
#         <stCode>%destination_station_code%</stCode>
#     </rsvStation>
#     <wag>
#         <wagTypeID>%wagon_type_code%</wagTypeID>
#         <wagCount>1</wagCount>
#     </wag>
# </TPIN_ServiceRequest>
# # """
