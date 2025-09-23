
-- Bật extension nếu chưa có
CREATE EXTENSION IF NOT EXISTS tablefunc;



-- tạo view để direct query vào PBI (bảng report_asm_final_table)
create or replace view project_final.view_report_asm as 
select *from report_asm_final_table 





-- Tạo view để query direct vào PBI ( bảng report_finance_table)
create or replace view project_final.view_report_finance as
with pivoted as (
    select 
        dps.sortorder,
        dps.pnl_name || '_' || a.month_key::text as row_name,
        b.area_name,
        a.amount
    from project_final.report_finance_table a
    left join project_final.dim_area_table b 
           on a.area_code = b.area_code
    left join project_final.dim_pnl_structure dps
           on a.id = dps.pnl_id
    order by dps.sortorder, a.month_key, b.area_name
),
xtab as (
    select *
    from crosstab(
        $$
        select 
            dps.pnl_name || '_' || a.month_key::text as row_name,
            b.area_name,
            a.amount
        from project_final.report_finance_table a
        left join project_final.dim_area_table b 
               on a.area_code = b.area_code
        left join project_final.dim_pnl_structure dps
               on a.id = dps.pnl_id
        order by dps.sortorder, a.month_key, b.area_name
        $$,
        $$
        select area_name from project_final.dim_area_table order by area_code
        $$
    ) as ct (
        row_name text,
        "Hội Sở" numeric,
        "Đông Bắc Bộ" numeric,
        "Tây Bắc Bộ" numeric,
        "Đồng Bằng Sông Hồng" numeric,
        "Bắc Trung Bộ" numeric,
        "Nam Trung Bộ" numeric,
        "Tây Nam Bộ" numeric,
        "Đông Nam Bộ" numeric
    )
)
select 
    split_part(x.row_name, '_', 2) as month_key,
    split_part(x.row_name, '_', 1) as pnl_name,
    dps.sortorder,                                -- thêm sortorder ở ngoài
    x."Hội Sở",
    x."Đông Bắc Bộ",
    x."Tây Bắc Bộ",
    x."Đồng Bằng Sông Hồng",
    x."Bắc Trung Bộ",
    x."Nam Trung Bộ",
    x."Tây Nam Bộ",
    x."Đông Nam Bộ"
from xtab x
join project_final.dim_pnl_structure dps
     on dps.pnl_name = split_part(x.row_name, '_', 1);


    
    
-- tạo thêm view để vẽ biểu đồ để so sánh các nhân sự ASM top đầu và cuối 
create or replace view view_heat_map as 
WITH ranked AS (
    SELECT
        month_key,
        email,
        area_name,
        tong_diem,
        rank_final,
        ROW_NUMBER() OVER (PARTITION BY month_key ORDER BY rank_final ASC) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY month_key ORDER BY rank_final DESC) AS rn_desc,
        ltn_avg, 
        psdn_avg,
        approval_rate_avg,
        npl_truoc_wo_luy_ke,
        ABS(cir) AS cir, 
        ABS(margin) AS margin
    FROM report_asm_final_table
)
SELECT
    r.month_key,
    r.email,
    r.rank_final as rank_for_sort,
    t.kpi,
    t.value
FROM ranked r
CROSS JOIN LATERAL (
    SELECT *
    FROM UNNEST(
        ARRAY['1.Tiền giải ngân','2.PSDN','3.Tỉ lệ phê duyệt','4.NPL trước WO','5.CIR'],
        ARRAY[r.ltn_avg, r.psdn_avg, r.approval_rate_avg, r.npl_truoc_wo_luy_ke, r.cir]
    ) AS u(kpi, value)
) t
WHERE r.rn_asc <= 3 OR r.rn_desc <= 3
ORDER BY r.month_key, r.rank_final, t.kpi;



create or replace view view_col_chart as
SELECT 
    a.month_key,
    b.pnl_name,
    c.area_name,
    a.amount 
      - COALESCE(
            LAG(a.amount) OVER (
                PARTITION BY b.pnl_id, c.area_name   -- nhóm theo chỉ tiêu & vùng
                ORDER BY a.month_key
            ), 
        0) AS monthly_amount         
FROM report_finance_table a
LEFT JOIN dim_pnl_structure b 
    ON a.id = b.pnl_id
LEFT JOIN dim_area_table c 
    ON a.area_code = c.area_code
WHERE a.id IN (1,20,21,26)
  AND a.area_code <> 'A'
ORDER BY a.month_key, b.pnl_id, c.area_name asc;


