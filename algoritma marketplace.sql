CREATE TEMP FUNCTION
  DaysInMonth(d DATE) AS ( 32 - EXTRACT(DAY
    FROM
      DATE_ADD(DATE_TRUNC(d, MONTH), INTERVAL 31 DAY)) );
CREATE TEMP FUNCTION
  lastDayInPrevMonth(t timestamp) AS (EXTRACT(day
    FROM
      DATE_SUB(DATE_TRUNC(CAST(t AS date), MONTH), INTERVAL 1 DAY)));
CREATE TEMP FUNCTION
  lastDayInCurMonth(t timestamp ) AS (EXTRACT(day
    FROM
      DATE_SUB(DATE_TRUNC(DATE_ADD(CAST(t AS date), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)));
CREATE TEMP FUNCTION
  days_1st_func(t timestamp) AS (EXTRACT(day
    FROM
      t) -1);


create table `my-2nd-skripsi.jt_2_new_shop_id.sum_sold_each_month` as 
SELECT
  shop_id,
  item_id,
  yearmonth,
  parent_name,
  sub_name,
  sub_sub_name,
  MAX(sold) AS sold30_adj,
  #calculate median prices
  MAX(pre_sold_adj) AS sold_adj,
  TIMESTAMP_SECONDS(CAST(AVG(UNIX_SECONDS(scraper_timestamp)) AS int64)) AS scraper_timestamp,
  AVG(price_agg) AS price,
FROM (
  SELECT
    *,
    PERCENTILE_CONT(price,
      0.5) OVER(PARTITION BY shop_id, item_id, yearmonth) AS price_agg,
  FROM (
    SELECT
      *
    FROM (
      SELECT
        *,
        CONCAT(CAST(EXTRACT(YEAR
            FROM
              scraper_timestamp) AS string), LPAD(CAST(EXTRACT(MONTH
              FROM
                scraper_timestamp) AS string),
            2,
            '0') ) AS yearmonth,
      IF
        (sold > sold_historical,
          sold,
          sold_historical ) AS pre_sold_adj
      FROM
        `my-2nd-skripsi.performance_check.jt_2_shop_id` )
    WHERE
      pre_sold_adj > 0 ) )
GROUP BY
  shop_id,
  item_id,
  yearmonth,
  parent_name,
  sub_name,
  sub_sub_name;

create table `my-2nd-skripsi.jt_2_new_shop_id.m_sold_fill_august` AS
 SELECT
          *
        FROM (
          SELECT
            * EXCEPT(prepre_next_m_sold,
              prepre_m_sold),
            m_sold_adj * price AS omset,
          FROM (
            SELECT
              *,
              ROUND(safe_divide(day_1st*m_sold,
                  days_m_sold) + (day_2nd*safe_divide(next_m_sold,
                    nextd_m_sold))) AS m_sold_adj,
            FROM (
              SELECT
                * EXCEPT (pre_nextd_m_sold),
              IF
                (pre_nextd_m_sold IS NULL,
                  days_m_sold,
                  pre_nextd_m_sold) AS nextd_m_sold
              FROM (
                SELECT
                  *,
                IF
                  (LEAD(yearmonth) OVER(PARTITION BY item_id ORDER BY yearmonth) IS NULL,
                    m_sold,
                    prepre_next_m_sold) AS next_m_sold
                FROM (
                  SELECT
                    lastDayInCurMonth(scraper_timestamp) - day_1st + EXTRACT(day
                    FROM
                      LEAD(scraper_timestamp) OVER(PARTITION BY item_id ORDER BY yearmonth)) pre_nextd_m_sold,
                  IF
                    (pre_next_m_sold IS NULL,
                      m_sold,
                      pre_next_m_sold) AS prepre_next_m_sold,
                    * EXCEPT(pre_next_m_sold)
                  FROM (
                    SELECT
                      *,
                      LEAD(m_sold) OVER(PARTITION BY item_id ORDER BY (yearmonth)) AS pre_next_m_sold,
                    FROM (
                      SELECT
                        *
                      FROM (
                        SELECT
                        IF
                          (preprepre_m_sold < 0,
                            0,
                            preprepre_m_sold) AS m_sold,
                          * EXCEPT(pre_days_m_sold),
                        FROM (
                          SELECT
                            *,
                          IF
                            (LAG(prepre_m_sold) OVER(PARTITION BY item_id ORDER BY yearmonth) IS NULL,
                              sold30_adj,
                              prepre_m_sold) AS preprepre_m_sold
                          FROM (
                            SELECT
                            IF
                              (pre_m_sold IS NULL,
                                sold_adj,
                                pre_m_sold) AS prepre_m_sold,
                              * EXCEPT(pre_m_sold),
                              days_1st_func(scraper_timestamp) AS day_1st,
                              DaysInMonth(CAST(scraper_timestamp AS date)) - days_1st_func(scraper_timestamp) + 1 AS day_2nd,
                              days_1st_func(scraper_timestamp) + lastDayInPrevMonth(scraper_timestamp) - EXTRACT(day
                              FROM
                                LAG(scraper_timestamp) OVER(PARTITION BY item_id ORDER BY (yearmonth))) +1 AS pre_days_m_sold,
                            IF
                              (pre_lag_tmp IS NULL,
                                30 + EXTRACT(day
                                FROM
                                  scraper_timestamp),
                                days_1st_func(scraper_timestamp) + lastDayInPrevMonth(scraper_timestamp) - pre_lag_tmp +1)AS days_m_sold
                            FROM (
                              SELECT
                                *,
                                EXTRACT(day
                                FROM
                                  LAG(scraper_timestamp) OVER(PARTITION BY item_id ORDER BY (yearmonth))) AS pre_lag_tmp,
                                sold_adj - LAG(sold_adj) OVER (PARTITION BY item_id ORDER BY yearmonth) AS pre_m_sold,
                              FROM
                                `my-2nd-skripsi.jt_2_new_shop_id.sum_sold_each_month`) ) )) ) ) ) )) ) );

create table `my-2nd-skripsi.jt_2_new_shop_id.im_aug` AS
SELECT
  shop_id,
  item_id,
  parent_name,
  sub_name,
  sub_sub_name,
  yearmonth,
  price,
  m_sold,
  m_sold_adj,
  m_sold_adj_final,
  m_sold_adj_final * price AS omset
FROM (
  SELECT
    * EXCEPT(scraper_timestamp_1,
      parent_name_1,
      sub_name_1,
      sub_sub_name_1,
      price_1,
      yearmonth_1 ),
  IF
    (yearmonth = '202009',
      ROUND(day_1st/days_m_sold * s_pre_m_sold),
      ROUND(s_pre_m_sold)) AS m_sold_adj_final,
  FROM (
    SELECT
      * EXCEPT(omset),
    IF
      (yearmonth = '202008'
        AND LEAD(yearmonth) OVER(PARTITION BY item_id ORDER BY yearmonth ) = '202009',
        ((LEAD(days_m_sold) OVER(PARTITION BY item_id ORDER BY yearmonth)) - (LEAD(day_1st) OVER(PARTITION BY item_id ORDER BY yearmonth)))/(LEAD(days_m_sold) OVER(PARTITION BY item_id ORDER BY yearmonth))* LEAD(m_sold_adj) OVER(PARTITION BY item_id ORDER BY yearmonth),
        m_sold_adj )AS s_pre_m_sold,
    IF
      (yearmonth = '202008',
        LEAD(parent_name_1) OVER(PARTITION BY item_id ORDER BY yearmonth),
        parent_name_1) AS parent_name,
    IF
      (yearmonth = '202008',
        LEAD(sub_name_1) OVER(PARTITION BY item_id ORDER BY yearmonth),
        sub_name_1 ) AS sub_name,
    IF
      (yearmonth = '202008',
        LEAD(sub_sub_name_1) OVER(PARTITION BY item_id ORDER BY yearmonth),
        sub_sub_name_1 ) AS sub_sub_name,
    IF
      (yearmonth = '202008',
        TIMESTAMP("2020-08-15 12:00:00+00"),
        scraper_timestamp_1) AS scraper_timestamp,
    IF
      (yearmonth = '202008',
        LEAD(price_1) OVER(PARTITION BY item_id ORDER BY yearmonth),
        price_1) AS price,
    FROM (
      SELECT
        * EXCEPT(scraper_timestamp,
          parent_name,
          sub_name,
          sub_sub_name,
          price),
        parent_name AS parent_name_1,
        sub_name AS sub_name_1,
        sub_sub_name AS sub_sub_name_1,
        price AS price_1,
        scraper_timestamp AS scraper_timestamp_1
      FROM (
        SELECT
          *
        FROM (
          SELECT
            shop_id,
            item_id,
            yearmonth
          FROM (
            SELECT
              shop_id,
              item_id,
              yearmonth
            FROM (
              SELECT
                *
              FROM
                `my-2nd-skripsi.jt_2_new_shop_id.m_sold_fill_august` )
            UNION ALL (
              SELECT
                DISTINCT shop_id,
                item_id,
                '202008' AS yearmonth
              FROM
                `my-2nd-skripsi.jt_2_new_shop_id.m_sold_fill_august` ) ) ) AS a
        FULL OUTER JOIN (
          SELECT
            * EXCEPT(shop_id,
              item_id,
              yearmonth),
            shop_id AS shop_id_1,
            item_id AS item_id_1,
            yearmonth AS yearmonth_1
          FROM
            `my-2nd-skripsi.jt_2_new_shop_id.m_sold_fill_august` ) AS b
        ON
          a.shop_id = b.shop_id_1
          AND a.item_id = b.item_id_1
          AND a.yearmonth = b.yearmonth_1 ) ) ) )
WHERE
  m_sold_adj_final IS NOT NULL
ORDER BY
  shop_id,
  item_id,
  yearmonth;

CREATE TABLE `my-2nd-skripsi.jt_2_new_shop_id.outlier_final` AS
SELECT
  *
FROM (
  SELECT
    * EXCEPT(lb_p,
      ub_p,
      lb_s,
      ub_s,
      Q_1p,
      Q_2p,
      Q_3p,
      Q_1s,
      Q_2s,
      Q_3s,
      lag_m_sold_adj),
  IF
    (price < lb_p
      OR price > ub_p,
      TRUE,
      FALSE) AS is_outlier_price,
  IF
    (ch_m_sold_adj < lb_s
      OR ch_m_sold_adj > ub_s,
      TRUE,
      FALSE) AS is_outlier_m_sold,
  FROM (
    SELECT
      *,
      Q_1p - (1.5*(Q_3p - Q_1p)) AS lb_p,
      Q_3p + (1.5*(Q_3p - Q_1p)) AS ub_p,
      Q_1s - (1.5*(Q_3s - Q_1s)) AS lb_s,
      Q_3s + (1.5*(Q_3s - Q_1s)) AS ub_s,
    FROM (
      SELECT
        *,
        percentile_cont(ch_m_sold_adj,
          0.25) OVER(PARTITION BY parent_name, sub_name, sub_sub_name, yearmonth) AS Q_1s,
        percentile_cont(ch_m_sold_adj,
          0.50) OVER(PARTITION BY parent_name, sub_name, sub_sub_name, yearmonth) AS Q_2s,
        percentile_cont(ch_m_sold_adj,
          0.75) OVER(PARTITION BY parent_name, sub_name, sub_sub_name, yearmonth) AS Q_3s,
      FROM (
        SELECT
          *,
          percentile_cont(price,
            0.25) OVER(PARTITION BY parent_name, sub_name, sub_sub_name, yearmonth) AS Q_1p,
          percentile_cont(price,
            0.50) OVER(PARTITION BY parent_name, sub_name, sub_sub_name, yearmonth) AS Q_2p,
          percentile_cont(price,
            0.75) OVER(PARTITION BY parent_name, sub_name, sub_sub_name, yearmonth) AS Q_3p,
          safe_divide(m_sold_adj,
            LAG(m_sold_adj) OVER(PARTITION BY item_id ORDER BY (yearmonth)) ) -1 AS ch_m_sold_adj,
          LAG(m_sold_adj) OVER(PARTITION BY item_id ORDER BY (yearmonth)) AS lag_m_sold_adj
        FROM (
          SELECT
            *
          FROM
            `my-2nd-skripsi.jt_2_new_shop_id.im_aug` ) ) )) )
WHERE
  is_outlier_m_sold = FALSE
  AND is_outlier_price = FALSE;

CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.seasonal_index_not_null` AS
SELECT
  *
FROM (
  SELECT
    DISTINCT shop_id,
    item_id,
    yearmonth,
    m_sold_adj_final,
    average_in_aYear,
    sum_m_sold_adj_final,
    c_yearmonth,
    safe_divide(m_sold_adj_final,
      average_in_aYear) AS seasonal_index
  FROM (
    SELECT
      *
    FROM (
      SELECT
        shop_id AS shop_id_1,
        item_id AS item_id_1,
        yearmonth,
        MAX(m_sold_adj_final) AS m_sold_adj_final
      FROM
        `my-2nd-skripsi.jt_2_new_shop_id.outlier_final`
      WHERE
        yearmonth = '202001'
        OR yearmonth = '202002'
        OR yearmonth = '202003'
        OR yearmonth = '202004'
        OR yearmonth = '202005'
        OR yearmonth = '202006'
        OR yearmonth = '202007'
        OR yearmonth = '202008'
        OR yearmonth = '202009'
        OR yearmonth = '202010'
        OR yearmonth = '202011'
        OR yearmonth = '202012'
      GROUP BY
        shop_id_1,
        item_id_1,
        yearmonth ) AS b
    LEFT JOIN (
      SELECT
        *
      FROM (
        SELECT
          shop_id,
          item_id,
          SUM(m_sold_adj_final) AS sum_m_sold_adj_final,
          COUNT(yearmonth) AS c_yearmonth,
          safe_divide(SUM(m_sold_adj_final),
            COUNT(yearmonth)) AS average_in_aYear
        FROM (
          SELECT
            DISTINCT shop_id,
            item_id,
            yearmonth,
            MAX(m_sold_adj_final) AS m_sold_adj_final
          FROM
            `my-2nd-skripsi.jt_2_new_shop_id.outlier_final`
          GROUP BY
            shop_id,
            item_id,
            yearmonth )
        WHERE
          yearmonth = '202001'
          OR yearmonth = '202002'
          OR yearmonth = '202003'
          OR yearmonth = '202004'
          OR yearmonth = '202005'
          OR yearmonth = '202006'
          OR yearmonth = '202007'
          OR yearmonth = '202008'
          OR yearmonth = '202009'
          OR yearmonth = '202010'
          OR yearmonth = '202011'
          OR yearmonth = '202012'
        GROUP BY
          shop_id,
          item_id ) ) AS a
    ON
      a.shop_id = b.shop_id_1
      AND a.item_id = b.item_id_1 ) )
WHERE
  seasonal_index IS NOT NULL;
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.variance` AS
SELECT
  *
FROM (
  SELECT
    shop_id,
    item_id,
    SUM(m_sold_adj_final) AS sum_m_sold_inYear,
    ROUND(AVG(average_in_aYear),2) AS avg_m_sold_adj_inYear,
    ROUND(AVG(seasonal_index*seasonal_index)-(AVG(seasonal_index)*AVG(seasonal_index)), 2) AS variance,
  FROM
    `my-2nd-skripsi.jt_2_new_shop_id.seasonal_index_not_null`
  GROUP BY
    shop_id,
    item_id );
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.seasonal_index_variance` AS
SELECT
  *
FROM
  `my-2nd-skripsi.jt_2_new_shop_id.seasonal_index_not_null` AS a
JOIN (
  SELECT
    * EXCEPT(shop_id,
      item_id),
    shop_id AS shop_id_1,
    item_id AS item_id_1
  FROM
    `my-2nd-skripsi.jt_2_new_shop_id.variance`) AS b
ON
  a.shop_id = b.shop_id_1
  AND a.item_id = b.item_id_1
ORDER BY
  shop_id,
  item_id,
  yearmonth;
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.parameter_1_2` AS
SELECT
  *,
IF
  (is_seasonal_item IS FALSE
    AND is_count_yearmonth_not_app IS FALSE,
    -1,
    1) AS is_parameter_1_2_not_app
FROM (
  SELECT
    *,
  IF
    (is_seasonal_item IS FALSE,
      -1,
      1) AS score_seasonal_item,
  IF
    (is_count_yearmonth_not_app IS FALSE,
      -1,
      1) AS score_yearmonth
  FROM (
    SELECT
      *,
    IF
      (variance > 3.25,
        TRUE,
        FALSE) is_seasonal_item,
    IF
      (c_yearmonth >= 5,
        FALSE,
        TRUE) is_count_yearmonth_not_app
    FROM (
      SELECT
        DISTINCT shop_id,
        item_id,
        variance,
        c_yearmonth,
        average_in_aYear
      FROM
        `my-2nd-skripsi.jt_2_new_shop_id.seasonal_index_variance`
      ORDER BY
        shop_id,
        item_id ) ));
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.parameter_1_2_shop_score` AS
SELECT
  *,
IF
  (shop_id_1 IS NULL,
    TRUE,
    FALSE) AS is_p1_p2_not_inc
FROM (
  SELECT
    *
  FROM (
    SELECT
      shop_id,
      SUM(is_parameter_1_2_not_app) AS s_p_1_pre,
      COUNT(item_id) AS c_item
    FROM (
      SELECT
        *
      FROM
        `my-2nd-skripsi.jt_2_new_shop_id.parameter_1_2` )
    GROUP BY
      shop_id ) AS a
  LEFT JOIN (
    SELECT
      shop_id AS shop_id_1,
      SUM(is_parameter_1_2_not_app) AS sum_p_1
    FROM (
      SELECT
        *
      FROM
        `my-2nd-skripsi.jt_2_new_shop_id.parameter_1_2`
      WHERE
        is_parameter_1_2_not_app != 1 )
    GROUP BY
      shop_id) AS b
  ON
    a.shop_id = b.shop_id_1 );
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.count_category` AS
SELECT
  DISTINCT shop_id,
  item_id,
  COUNT(DISTINCT parent_name) OVER(PARTITION BY shop_id, item_id) AS c_parent_name,
  COUNT(DISTINCT sub_name) OVER(PARTITION BY shop_id, item_id) AS c_sub_name,
  COUNT(DISTINCT sub_sub_name) OVER(PARTITION BY shop_id, item_id) AS c_sub_sub_name,
  COUNT(DISTINCT name) OVER(PARTITION BY shop_id, item_id) AS c_name,
  parent_name,
  sub_name,
  sub_sub_name,
  name
FROM (
  SELECT
    DISTINCT * EXCEPT(shop_id_1,
      item_id_1 ),
  FROM (
    SELECT
      DISTINCT shop_id,
      item_id
    FROM
      `my-2nd-skripsi.jt_2_new_shop_id.outlier_final` ) AS a
  LEFT JOIN (
    SELECT
      shop_id AS shop_id_1,
      item_id AS item_id_1,
      parent_name,
      sub_name,
      sub_sub_name,
      name,
      CONCAT(CAST(EXTRACT(YEAR
          FROM
            scraper_timestamp) AS string), LPAD(CAST(EXTRACT(MONTH
            FROM
              scraper_timestamp) AS string),
          2,
          '0') ) AS yearmonth,
    FROM
      `skripsiwiwin.merged_dataset_1.item_merged_with_des_2019`) AS b
  ON
    a.shop_id = b.shop_id_1
    AND a.item_id = b.item_id_1
  WHERE
    yearmonth != '202101'
    OR yearmonth != '201912' );
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.parameter_3` AS
SELECT
  *,
IF
  (is_items_not_unique IS FALSE,
    -1,
    1) AS score_item_ganti2
FROM (
  SELECT
    *,
  IF
    (c_name >= 2
      AND c_sub_name >= 3
      AND c_sub_sub_name >= 3
      AND c_name >= 4,
      TRUE,
      FALSE) AS is_items_not_unique
  FROM
    `my-2nd-skripsi.jt_2_new_shop_id.count_category` );
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.parameter_3_score_shop` AS
SELECT
  *,
IF
  (pre_score_parameter_3_shop < 0,
    FALSE,
    TRUE) AS is_parameter_3_not_inc
FROM (
  SELECT
    shop_id,
    SUM(score_item_ganti2) AS pre_score_parameter_3_shop
  FROM
    `my-2nd-skripsi.jt_2_new_shop_id.parameter_3`
  GROUP BY
    shop_id);
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.join_p_12_p3` AS
SELECT
  *,
IF
  (is_p1_p2_not_inc IS FALSE
    AND is_parameter_3_not_inc IS FALSE,
    TRUE,
    FALSE) AS is_active_shop
FROM (
  SELECT
    * EXCEPT(shop_id_1)
  FROM
    `my-2nd-skripsi.jt_2_new_shop_id.parameter_1_2_shop_score` AS a
  JOIN (
    SELECT
      * EXCEPT(shop_id),
      shop_id AS shop_id_1
    FROM
      `my-2nd-skripsi.jt_2_new_shop_id.parameter_3_score_shop` ) AS b
  ON
    a.shop_id = b.shop_id_1);
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.toko_aktif_filter_item` AS
SELECT
  DISTINCT shop_id,
  item_id
FROM (
  SELECT
    shop_id
  FROM
    `my-2nd-skripsi.jt_2_new_shop_id.join_p_12_p3`
  WHERE
    is_active_shop = TRUE) AS a
LEFT JOIN (
  SELECT
    * EXCEPT(shop_id),
    shop_id AS shop_id_2
  FROM (
    SELECT
      * EXCEPT(shop_id_1,
        item_id_1 )
    FROM (
      SELECT
        shop_id,
        item_id,
        is_parameter_1_2_not_app
      FROM
        `my-2nd-skripsi.jt_2_new_shop_id.parameter_1_2`) AS a
    JOIN (
      SELECT
        shop_id AS shop_id_1,
        item_id AS item_id_1,
        score_item_ganti2
      FROM
        `my-2nd-skripsi.jt_2_new_shop_id.parameter_3` ) AS b
    ON
      a.shop_id = b.shop_id_1
      AND a.item_id = b.item_id_1
    ORDER BY
      shop_id,
      item_id )
  WHERE
    is_parameter_1_2_not_app = -1
    AND score_item_ganti2 = -1) AS b
ON
  a.shop_id = b.shop_id_2;
CREATE TABLE `my-2nd-skripsi.jt_2_new_shop_id.data_toko_aktif_with_item_atribut` AS
SELECT
  DISTINCT shop_id,
  item_id,
  parent_name,
  sub_name,
  sub_sub_name,
  yearmonth,
  price,
  m_sold_adj_final,
  omset
FROM (
  SELECT
    * EXCEPT(shop_id_1,
      item_id_1 )
  FROM (
    SELECT
      *
    FROM
      `my-2nd-skripsi.jt_2_new_shop_id.toko_aktif_filter_item` AS a
    LEFT JOIN (
      SELECT
        * EXCEPT(shop_id,
          item_id),
        shop_id AS shop_id_1,
        item_id AS item_id_1
      FROM
        `my-2nd-skripsi.jt_2_new_shop_id.outlier_final`) AS b
    ON
      a.shop_id = b.shop_id_1
      AND a.item_id = b.item_id_1 )
  WHERE
    yearmonth = '202001'
    OR yearmonth = '202002'
    OR yearmonth = '202003'
    OR yearmonth = '202004'
    OR yearmonth = '202005'
    OR yearmonth = '202006'
    OR yearmonth = '202007'
    OR yearmonth = '202008'
    OR yearmonth = '202009'
    OR yearmonth = '202010'
    OR yearmonth = '202011'
    OR yearmonth = '202012'
  ORDER BY
    shop_id,
    item_id,
    yearmonth );

CREATE TABLE `my-2nd-skripsi.jt_2_new_shop_id.omset_toko_bulanan` AS
SELECT
  shop_id,
  COUNT(item_id) AS c_item_id,
  yearmonth,
  SUM(m_sold_adj_final) AS sum_m_sold_monthly,
  SUM(omset) AS sum_omset_monthly
FROM
  `my-2nd-skripsi.jt_2_new_shop_id.data_toko_aktif_with_item_atribut`
GROUP BY
  shop_id,
  yearmonth
;

CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.seasonal_index_toko` AS
SELECT
  DISTINCT shop_id,
  yearmonth,
  sum_omset_monthly,
  sum_sum_omset_monthly,
  average_in_aYear,
  c_yearmonth,
  safe_divide(sum_omset_monthly,
    average_in_aYear) AS seasonal_index
FROM (
  SELECT
    *
  FROM (
    SELECT
      shop_id AS shop_id_1,
      yearmonth,
      MAX(sum_omset_monthly) AS sum_omset_monthly
    FROM
      `my-2nd-skripsi.jt_2_new_shop_id.omset_toko_bulanan`
    GROUP BY
      shop_id_1,
      yearmonth ) AS b
  LEFT JOIN (
    SELECT
      *
    FROM (
      SELECT
        shop_id,
        SUM(sum_omset_monthly ) AS sum_sum_omset_monthly,
        COUNT(yearmonth) AS c_yearmonth,
        safe_divide(SUM(sum_omset_monthly ),
          COUNT(yearmonth)) AS average_in_aYear
      FROM (
        SELECT
          DISTINCT shop_id,
          yearmonth,
          MAX(sum_omset_monthly) AS sum_omset_monthly
        FROM
          `my-2nd-skripsi.jt_2_new_shop_id.omset_toko_bulanan`
        GROUP BY
          shop_id,
          yearmonth )
      GROUP BY
        shop_id ) ) AS a
  ON
    a.shop_id = b.shop_id_1 )
ORDER BY
  shop_id,
  yearmonth;
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.variance_omset_merged` AS
SELECT
  DISTINCT * EXCEPT(shop_id_1)
FROM (
  SELECT
    shop_id AS shop_id_1,
    ROUND(AVG(seasonal_index*seasonal_index) - (AVG(seasonal_index)*AVG(seasonal_index)), 2) AS variance
  FROM
    `my-2nd-skripsi.jt_2_new_shop_id.seasonal_index_toko`
  GROUP BY
    shop_id
  ORDER BY
    variance DESC) AS a
LEFT JOIN (
  SELECT
    *
  FROM
    `my-2nd-skripsi.jt_2_new_shop_id.data_toko_aktif_with_item_atribut`) AS b
ON
  a.shop_id_1 = b.shop_id
ORDER BY
  variance DESC,
  shop_id,
  item_id,
  yearmonth;
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.range_price` AS
SELECT
  *
FROM (
  SELECT
    *,
    ROUND((1- (min_price/max_price))*100, 2) AS perc_range
  FROM (
    SELECT
      *,
      MAX(price) OVER(PARTITION BY shop_id, item_id) AS max_price,
      MIN(price) OVER(PARTITION BY shop_id, item_id) AS min_price,
      percentile_cont(price,
        0.75) OVER(PARTITION BY shop_id, item_id) AS q_3,
      percentile_cont(price,
        0.5) OVER(PARTITION BY shop_id, item_id) AS q_2,
      percentile_cont(price,
        0.25) OVER(PARTITION BY shop_id, item_id) AS q_1,
    FROM
      `my-2nd-skripsi.jt_2_new_shop_id.variance_omset_merged` ))
WHERE
  perc_range <= 98
ORDER BY
  perc_range DESC,
  item_id,
  yearmonth;
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.final_parameter_with_price_flags` AS
SELECT
  *,
IF
  (perc_range >= 95,
    TRUE,
    FALSE) AS is_price_anom
FROM (
  SELECT
    *,
    COUNT(DISTINCT parent_name) OVER(PARTITION BY shop_id, item_id) AS c_parent_name,
    COUNT(DISTINCT sub_name) OVER(PARTITION BY shop_id, item_id) AS c_sub_name,
    COUNT(DISTINCT sub_sub_name) OVER(PARTITION BY shop_id, item_id) AS c_sub_sub_name,
  FROM
    `my-2nd-skripsi.jt_2_new_shop_id.range_price` )
WHERE
  c_parent_name <= 2
  AND c_sub_name <= 3
  AND c_sub_sub_name <= 3
ORDER BY
  perc_range DESC,
  item_id,
  yearmonth;
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.shop_price_cleaned` AS
SELECT
  *,
  ROUND(AVG(seasonal_ratio_new *seasonal_ratio_new ) OVER(PARTITION BY shop_id) - (AVG(seasonal_ratio_new ) OVER(PARTITION BY shop_id) *AVG(seasonal_ratio_new ) OVER(PARTITION BY shop_id)), 5) AS var_omset_new
FROM (
  SELECT
    *,
    safe_divide(SUM(omset) OVER(PARTITION BY shop_id, yearmonth),
      avg_new) AS seasonal_ratio_new
  FROM (
    SELECT
      *,
      SUM(omset_bulanan) OVER(PARTITION BY shop_id)/COUNT(DISTINCT yearmonth) OVER(PARTITION BY shop_id) AS avg_new
    FROM (
      SELECT
        *,
        SUM(omset) OVER(PARTITION BY shop_id, yearmonth) AS omset_bulanan
      FROM
        `my-2nd-skripsi.jt_2_new_shop_id.final_parameter_with_price_flags` ) ))
ORDER BY
  var_omset_new DESC;
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.price_not_anom_df` AS
SELECT
  * EXCEPT(is_price_anom),
  CASE yearmonth
    WHEN '202001' THEN FORMAT_DATE("%b %Y", DATE "2020-01-01")
    WHEN '202002' THEN FORMAT_DATE("%b %Y", DATE "2020-02-01")
    WHEN '202003' THEN FORMAT_DATE("%b %Y", DATE "2020-03-01")
    WHEN '202004' THEN FORMAT_DATE("%b %Y", DATE "2020-04-01")
    WHEN '202005' THEN FORMAT_DATE("%b %Y", DATE "2020-05-01")
    WHEN '202006' THEN FORMAT_DATE("%b %Y", DATE "2020-06-01")
    WHEN '202007' THEN FORMAT_DATE("%b %Y", DATE "2020-07-01")
    WHEN '202008' THEN FORMAT_DATE("%b %Y", DATE "2020-08-01")
    WHEN '202009' THEN FORMAT_DATE("%b %Y", DATE "2020-09-01")
    WHEN '202010' THEN FORMAT_DATE("%b %Y", DATE "2020-10-01")
    WHEN '202011' THEN FORMAT_DATE("%b %Y", DATE "2020-11-01")
    WHEN '202012' THEN FORMAT_DATE("%b %Y", DATE "2020-12-01")
END
  AS year_month
FROM
  `my-2nd-skripsi.jt_2_new_shop_id.final_parameter_with_price_flags`
WHERE
  is_price_anom = FALSE
ORDER BY
  shop_id,
  item_id,
  yearmonth;
CREATE TABLE
  `my-2nd-skripsi.jt_2_new_shop_id.direktori_final` AS
SELECT
  * EXCEPT(shop_id_1 )
FROM (
  SELECT
    *,
    CASE
      WHEN annual_turnover <= 300000000 THEN 'mikro'
      WHEN annual_turnover > 300000000
    AND annual_turnover <= 2500000000 THEN 'kecil'
      WHEN annual_turnover > 2500000000 AND annual_turnover <= 50000000000 THEN 'menengah'
    ELSE
    'besar'
  END
    AS umkm
  FROM (
    SELECT
      shop_id,
      COUNT(DISTINCT item_id) AS num_item,
      SUM(omset) AS annual_turnover
    FROM
      `my-2nd-skripsi.jt_2_new_shop_id.price_not_anom_df`
    GROUP BY
      shop_id
    ORDER BY
      annual_turnover DESC,
      num_item ASC )) AS a
JOIN (
  SELECT
    shop_id AS shop_id_1,
    username,
    name,
    provinsi,
    kabupaten,
    kecamatan,
    tipe_toko
  FROM
    `my-2nd-skripsi.diskusi_19_Mei_2021.shop_merged_all`) AS b
ON
  a.shop_id = b.shop_id_1;

