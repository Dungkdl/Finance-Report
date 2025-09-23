-- Tạo procedure để tính tỉ lệ phân bổ head theo từng trường hợp
CREATE OR REPLACE PROCEDURE rate_area_month(p_rate_month int4)
AS $$
BEGIN 
	  -- ---------------------
    -- THÔNG TIN NGƯỜI TẠO
    -- ---------------------
    -- Tên người tạo: Mai Quang Dũng
    -- Ngày tạo: 25-08-2025

    -- ---------------------
    -- THÔNG TIN NGƯỜI CẬP NHẬT
    -- ---------------------
    -- Tên người cập nhật: Ten_Nguoi_Cap_Nhat
    -- Ngày cập nhật: current_timestamp
    -- Mục đích cập nhật: Mô tả mục đích sửa đổi, nâng cấp, hoặc sửa lỗi

    -- ---------------------
    -- SUMMARY LUỒNG XỬ LÝ
    -- ---------------------
    -- Bước 1: Xóa dữ liệu cũ trong bảng rate_allocation_area_month cho kỳ (p_rate_month) 
    --         để đảm bảo dữ liệu chạy lại không bị trùng.
    --
    -- Bước 2: Tạo các bảng tạm để phục vụ tính toán:
    --            * tmp_write_off_by_area     : tính giá trị write-off lũy kế theo từng khu vực (area_code) và tháng (month_key).
    --            * tmp_oustanding_principal  : tính dư nợ cuối kỳ (outstanding_principal) lũy kế cho các nhóm 2–5.
    --
    -- Bước 3: Thực hiện INSERT INTO rate_allocation_area_month bằng UNION ALL
    --         bao gồm các trường hợp phân bổ:
    --            3.1  AWO       : Dư nợ cuối kỳ bình quân sau WO (all group, gồm NULL).
    --            3.2  AWO_GR1   : Dư nợ cuối kỳ bình quân sau WO nhóm 1.
    --            3.3  AWO_GR2   : Dư nợ cuối kỳ bình quân sau WO nhóm 2.
    --            3.4  AWO_GR2_5 : Dư nợ cuối kỳ bình quân sau WO nhóm 2–5.
    --            3.5  BWO_GR2_5 : Dư nợ cuối kỳ bình quân trước WO nhóm 2–5 
    --                          (tính bằng outstanding(tháng đó ) + WO lũy kế tính từ đầu năm).
    --            3.6  SM_COUNT  : Phân bổ theo số lượng Sale Manager của từng khu vực. -> dựa vào bảng fact_kpi_asm 
    --            3.7  WO_PSDN   : Phân bổ theo số lượng thẻ phát sinh dư nợ (PSDN).
    --            3.8  Đồng thời thêm bản ghi HEAD = 0 cho khu vực 'A' (Hội sở). -> Để join cho tiện  
    --
    -- Bước 4: Kết thúc procedure, dữ liệu phân bổ cho từng phương pháp 
    --         được lưu tại bảng rate_allocation_area_month => Phục vụ cho việc chia tỉ lệ Head về các khu vực  


    -- ---------------------
    -- CHI TIẾT CÁC BƯỚC
    -- --------------------------------------------------------------------------
	
-- Bước 1 : 
    -- Xoá dữ liệu cũ của tháng cần chạy
    	DELETE FROM rate_allocation_area_month WHERE rate_month = p_rate_month;
-- Bước 2 :	
	-- tạo bảng tạm tính ra giá trị WO_lũy kế theo từng tháng 
		DROP TABLE IF EXISTS tmp_write_off_by_area;
		DROP TABLE IF EXISTS tmp_oustanding_principal;

		 create table tmp_write_off_by_area as 
		select y.month_key , y.area_code, sum(x.total_wo_bal_current) as total_lk 
			from 
			(
				select a.kpi_month as month_key, b.area_code  , sum(write_off_balance_principal) as total_wo_bal_current
				FROM fact_kpi_month_raw_data AS a
				JOIN dim_province_table AS b ON a.pos_city = b.province_name
				WHERE a.write_off_month = a.kpi_month
				group by a.kpi_month , b.area_code 
			) x 
			join 
			(
				select a.kpi_month as month_key, b.area_code  , sum(write_off_balance_principal) as total_wo_bal_current
				FROM fact_kpi_month_raw_data AS a
				JOIN dim_province_table AS b ON a.pos_city = b.province_name
				WHERE a.write_off_month = a.kpi_month
				group by a.kpi_month , b.area_code 
			) y on x.area_code = y.area_code and x.month_key <= y.month_key
			 -- where y.month_key = 202302
			-- and y.area_code = 'B'
			group by y.month_key , y.area_code ;
-- tạo bảng tạm tính ra giá trị outstanding lũy kế theo từng tháng của nhóm 2-5 
		create table tmp_oustanding_principal as 
				select a.kpi_month ,b.area_code ,sum(outstanding_principal) as amount 
					from fact_kpi_month_raw_data as a 
					left join dim_province_table as b on a.pos_city = b.province_name 
					where max_bucket in (2,3,4,5) 
					group by b.area_code , a.kpi_month  
					order by a.kpi_month ;

-- Bước 3 
    -- Lưu trữ tất cả các phương pháp phân bổ vào 1 lần insert bằng UNION ALL
    INSERT INTO rate_allocation_area_month (rate_month, method_code, method_name, area_code, ratio)
-- Bước 3.1 
    SELECT * FROM (
        -- Lưu trữ tỉ lệ phân bổ về từng khu vực theo DNCK sau WO (cần lấy thêm giá trị của nhóm NULL) 
        -- => chỉ cần sum các giá trị outstanding_principal là ra các giá trị sau WO
        SELECT
            p_rate_month AS rate_month,
            'AWO' AS method_code,
            'Phân bổ về từng KV theo DNCK bình quân sau WO' AS method_name,
            b.area_code AS area_code,
            SUM(a.outstanding_principal)::numeric
              / NULLIF(SUM(SUM(a.outstanding_principal)) OVER (), 0) AS ratio
        FROM fact_kpi_month_raw_data AS a
        LEFT JOIN dim_province_table AS b ON a.pos_city = b.province_name
        WHERE a.kpi_month <= p_rate_month
        GROUP BY b.area_code

        UNION all
        
-- Bước 3.2
        -- Lưu trữ tỉ lệ phân bổ về từng khu vực theo DNCK sau WO của nhóm 1 (cần lấy thêm giá trị của nhóm NULL)
        SELECT
            p_rate_month AS rate_month,
            'AWO_GR1' AS method_code,
            'Phân bổ về từng KV theo DNCK bình quân sau WO của nhóm 1' AS method_name,
            b.area_code AS area_code,
            SUM(a.outstanding_principal)::numeric
              / NULLIF(SUM(SUM(a.outstanding_principal)) OVER (), 0) AS ratio
        FROM fact_kpi_month_raw_data AS a
        LEFT JOIN dim_province_table AS b ON a.pos_city = b.province_name
        WHERE a.kpi_month <= p_rate_month
          AND (a.max_bucket = 1 OR a.max_bucket IS NULL)
        GROUP BY b.area_code

        UNION all
-- Bước 3.3 
        -- Lưu trữ tỉ lệ phân bổ về từng khu vực theo DNCK sau WO của nhóm 2
        SELECT
            p_rate_month AS rate_month,
            'AWO_GR2' AS method_code,
            'Phân bổ về từng KV theo DNCK bình quân sau WO của nhóm 2' AS method_name,
            b.area_code AS area_code,
            SUM(a.outstanding_principal)::numeric
              / NULLIF(SUM(SUM(a.outstanding_principal)) OVER (), 0) AS ratio
        FROM fact_kpi_month_raw_data AS a
        LEFT JOIN dim_province_table AS b ON a.pos_city = b.province_name
        WHERE a.kpi_month <= p_rate_month
          AND a.max_bucket = 2
        GROUP BY b.area_code

        UNION all
 -- Bước 3.4
      
        -- Lưu trữ tỉ lệ phân bổ về từng khu vực theo DNCK sau WO của nhóm 2-5 
        -- cần chỉ cần lấy các giá trị outstanding_principal của nhóm 2,3,4,5 
        SELECT
            p_rate_month AS rate_month,
            'AWO_GR2_5' AS method_code,
            'Phân bổ về từng KV theo DNCK bình quân sau WO của nhóm 2 đến nhóm 5' AS method_name,
             b.area_code AS area_code,
            SUM(a.outstanding_principal)::numeric
              / NULLIF(SUM(SUM(a.outstanding_principal)) OVER (), 0) AS ratio
        FROM fact_kpi_month_raw_data AS a
        LEFT JOIN dim_province_table AS b ON a.pos_city = b.province_name
        WHERE a.kpi_month <= p_rate_month
          AND a.max_bucket IN (2,3,4,5)
        GROUP BY b.area_code

        UNION all 
-- Bước 3.5        
        -- Lưu trữ tỉ lệ phân bổ về từng khu vực theo DNCK trước WO của nhóm 2-5 
        -- Lấy tất cả các giá trị của cột outstanding_principal  của nhóm từ 2-5 , lấy giá trị WO từ đầu năm đến tháng lũy kế (tất cả các nhóm )  = total
        -- (total_th1+total_th2 )/2 = ...
        SELECT
            p_rate_month AS rate_month,
            'BWO_GR2_5' AS method_code,
            'Phân bổ về từng KV theo DNCK bình quân trước WO của nhóm 2 đến nhóm 5' AS method_name,
            b.area_code AS area_code, 
				    (SUM(a.amount) + SUM(b.total_lk)) * 1.0 
				      / SUM(SUM(a.amount) + SUM(b.total_lk)) OVER () AS ratio
				FROM tmp_oustanding_principal AS a
				LEFT JOIN tmp_write_off_by_area AS b 
				       ON a.kpi_month = b.month_key  
				      AND b.area_code = a.area_code 
				WHERE a.kpi_month <= p_rate_month
				GROUP BY  b.area_code
        UNION all
        
-- Bước 3.6 
-- Phân bổ theo số lượng Sale manager của các khu vực 
	SELECT
            p_rate_month AS rate_month,
            'SM_COUNT'   AS method_code,
            'Phân bổ theo số lượng SM trong khu vực' AS method_name,
            b.area_code AS area_code,
            COUNT(DISTINCT a.sale_name)::numeric
              / NULLIF(SUM(COUNT(DISTINCT a.sale_name)) OVER (), 0) AS ratio
        FROM fact_kpi_asm as a
        LEFT JOIN dim_area_table as b ON a.area_name = b.area_name
        -- chỉ lấy các nhân viên đã làm việc bắt đầu từ tháng đó (dữ liệu được ghi nhân ở tháng thứ 5)
		WHERE p_rate_month % 100 = a.month_col and a.loan_to_new is not null 
        GROUP BY b.area_code

-- Phân bổ theo PSDN của các khu vực 
union all 
-- Bước 3.7 
        SELECT
            p_rate_month AS rate_month,
            'WO_PSDN' AS method_code,
            'Phân bổ về từng KV theo số lượng thẻ phát sinh dư nợ ' AS method_name,
             b.area_code AS area_code,
            SUM(a.psdn)::numeric
              / NULLIF(SUM(SUM(a.psdn)) OVER (), 0) AS ratio
        FROM fact_kpi_month_raw_data AS a
        LEFT JOIN dim_province_table AS b ON a.pos_city = b.province_name
        WHERE a.kpi_month <= p_rate_month
        GROUP BY b.area_code
UNION all
-- Bước 3.8 
        -- Thêm HEAD = 0 cho từng method_code
        SELECT p_rate_month, m.method_code, m.method_name, 'A'::char(1), 0::numeric
        FROM (
            VALUES
                ('AWO','Phân bổ về từng KV theo DNCK bình quân sau WO'),
                ('AWO_GR1','Phân bổ về từng KV theo DNCK bình quân sau WO nhóm 1'),
                ('AWO_GR2','Phân bổ về từng KV theo DNCK bình quân sau WO nhóm 2'),
                ('AWO_GR2_5','Phân bổ về từng KV theo DNCK bình quân sau WO nhóm 2-5'),
                ('BWO_GR2_5','Phân bổ về từng KV theo DNCK bình quân trước WO nhóm 2-5'),
                ('SM_COUNT','Phân bổ theo số lượng SM trong khu vực'),
                ('WO_PSDN','Phân bổ theo số lượng thẻ phát sinh dư nợ')
        ) AS m(method_code, method_name)
    ) t;
        
END
$$ LANGUAGE plpgsql;





-- Tạo procedure để tạo ra báo cáo tài chính doanh nghiệp theo từng tháng(lũy kế)
CREATE OR REPLACE PROCEDURE report_finance(fin_month int4)
LANGUAGE plpgsql
AS $$
DECLARE
    f_total numeric;
    transaction_month date;
begin
-- ---------------------
    -- THÔNG TIN NGƯỜI TẠO
    -- ---------------------
    -- Tên người tạo: Mai Quang Dũng
    -- Ngày tạo: 25-08-2025

    -- ---------------------
    -- THÔNG TIN NGƯỜI CẬP NHẬT
    -- ---------------------
    -- Tên người cập nhật: Ten_Nguoi_Cap_Nhat
    -- Ngày cập nhật: current_timestamp
    -- Mục đích cập nhật: Mô tả mục đích sửa đổi, nâng cấp, hoặc sửa lỗi
    -- ---------------------
    -- SUMMARY LUỒNG XỬ LÝ
    -- ---------------------
    -- Bước 1: Khởi tạo dữ liệu cho kỳ báo cáo (fin_month)
    --         * Xóa dữ liệu cũ trong bảng report_finance_table cho tháng fin_month
    --           để tránh trùng lặp khi chạy lại.
    --         * Chuẩn hóa tham số fin_month → xác định transaction_month
    --           (ngày cuối cùng của tháng fin_month).
    --
    -- Bước 2: Tính toán và insert dữ liệu cho các chỉ tiêu cấp chi tiết (Level 2)
    --         * Thu nhập từ hoạt động thẻ:
    --              - (3) Lãi trong hạn
    --              - (4) Lãi quá hạn
    --              - (5) Phí bảo hiểm
    --              - (6) Phí tăng hạn mức
    --              - (7) Phí thanh toán chậm, thu ngoại bảng
    --         * Chi phí thuần KDV:
    --              - (9) Doanh thu nguồn vốn
    --              - (10) Chi phí vốn CCTG
    --              - (11) CP vốn TT1
    --              - (12) CP vốn TT2
    --         * Chi phí hoạt động khác:
    --              - (14) Doanh thu Fintech
    --              - (15) Doanh thu tiểu thương, cá nhân
    --              - (16) Doanh thu kinh doanh
    --              - (17) CP hoa hồng
    --              - (18) CP thuần KD khác
    --              - (19) CP hợp tác kinh doanh tàu (net)
    --         * Chi phí khác:
    --              - (22) CP thuế, phí
    --              - (23) CP nhân viên
    --              - (24) CP quản lý
    --              - (25) CP tài sản
    --         * Dự phòng:
    --              - (26) Chi phí dự phòng
    --
    -- Bước 3: Tính toán và insert các chỉ tiêu tổng hợp (Level 1) = tổng của các level 2 (có parent_id = id của level 1 )
    --         * (2)  Tổng thu nhập từ hoạt động thẻ
    --         * (8)  Chi phí thuần KDV
    --         * (13) Chi phí thuần hoạt động khác
    --         * (20) Tổng thu nhập hoạt động
    --         * (21) Tổng chi phí hoạt động
    --         * (27) Số lượng nhân sự (Sale Manager)
    --
    -- Bước 4: Tính toán và insert chỉ tiêu Level 0
    --         * (1)  Lợi nhuận trước thuế
    --
    -- Bước 5: Tính toán và insert các chỉ số tài chính (P3)
    --         * (28) Chỉ số tài chính tổng hợp (placeholder)
    --         * (29) CIR (%)   = Chi phí hoạt động / Thu nhập hoạt động
    --         * (30) Margin (%) = LNTT / Doanh thu
    --         * (31) Hiệu suất trên vốn (%) = LNTT / Chi phí KDV
    --         * (32) Hiệu suất bình quân / nhân sự = LNTT / số lượng SM
    --
    -- Bước 6: Kết thúc procedure, dữ liệu báo cáo tài chính chi tiết theo khu vực
    --         và cấp bậc chỉ tiêu được lưu tại bảng report_finance_table.

    -- ---------------------
    -- CHI TIẾT CÁC BƯỚC
    -- --------------------------------------------------------------------------
-- Bước 1 
	-- Xóa dữ liệu cũ của tháng cần chạy 
	   DELETE FROM report_finance_table WHERE month_key = fin_month;
	    	
    -- Chuyển tháng kiểu int thành ngày cuối cùng của tháng đó 
    transaction_month := (to_date(fin_month::text, 'YYYYMM') 
                          + interval '1 month' - interval '1 day')::date;
-- Bước 2 
-- LEVEL 2 :
                         
-- Lãi trong hạn 
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('702000030002', '702000030001','702000030102')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT fin_month as month_key ,
    	   3 AS id,
           b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount

    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code 
          AND a.account_code IN ('702000030002', '702000030001','702000030102')
          AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'AWO_GR1' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;

-- Lãi quá hạn 
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('702000030012', '702000030112')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT fin_month as month_key ,
    		4 AS id,
            b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount

    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code
          AND a.account_code IN ('702000030012', '702000030112')
          AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'AWO_GR2' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;

-- Phí bảo hiểm 
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('716000000001')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT  fin_month as month_key ,
    		5 AS id,
            b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount

    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code
          AND a.account_code IN ('716000000001')
          AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'WO_PSDN' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;

-- Phí tăng hạn mức 
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('719000030002')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT  fin_month as month_key ,
    		6 AS id,
            b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code
          AND a.account_code IN ('719000030002')
          AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'AWO_GR1' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;

-- Phí thanh toán chậm , thu từ ngoại bảng 
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('719000030003','719000030103',
                           '790000030003','790000030103',
                           '790000030004','790000030104')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT  fin_month as month_key ,
    		7 AS id,
            b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code
          AND a.account_code IN ('719000030003','719000030103',
                                 '790000030003','790000030103',
                                 '790000030004','790000030104')
          AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'AWO_GR2_5' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;
-- DT Nguồn vốn 
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('Account_id')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT  fin_month as month_key ,
    		9 AS id,
            b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code
          AND a.account_code IN ('Account_id')
    AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'AWO' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;

-- Chi phí vốn CCTG
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('803000000001')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT  fin_month as month_key ,
    		10 AS id,
            b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code
          AND a.account_code IN ('803000000001')
          AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'AWO' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;

-- CP vốn TT1
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('802000000002','802000000003',
                           '802014000001','802037000001')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT  fin_month as month_key ,
    		11 AS id,
            b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code
          AND a.account_code IN ('802000000002','802000000003',
                                 '802014000001','802037000001')
          AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'AWO' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;

-- CP vốn TT2
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('801000000001','802000000001')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT  fin_month as month_key ,
    		12 AS id,
            b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code
          AND a.account_code IN ('801000000001','802000000001')
          AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'AWO' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;
	-- DT Fintech
	SELECT SUM(amount)
	INTO f_total
	FROM fact_txn_month_data
	WHERE account_code IN ('<nếu có GL code>')  -- hiện tại chưa có, sẽ để trống
	  AND transaction_date <= transaction_month
	  AND area_code = '00';
	
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			14 AS id,
	        b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
	FROM rate_allocation_area_month b
	LEFT JOIN fact_txn_month_data a
	       ON a.area_code = b.area_code
	      AND a.account_code IN ('<nếu có GL code>')
	      AND a.transaction_date <= transaction_month
	WHERE b.method_code = 'AWO' AND b.rate_month = fin_month
	GROUP BY b.area_code, b.ratio
	ORDER BY b.area_code;
	-- DT tiểu thương, cá nhân
	SELECT SUM(amount)
	INTO f_total
	FROM fact_txn_month_data
	WHERE account_code IN ('<nếu có GL code>')  -- hiện tại chưa có, sẽ để trống
	  AND transaction_date <= transaction_month
	  AND area_code = '00';
	
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			15 AS id,
	        b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
	FROM rate_allocation_area_month b
	LEFT JOIN fact_txn_month_data a
	       ON a.area_code = b.area_code
	      AND a.account_code IN ('<nếu có GL code>')
	      AND a.transaction_date <= transaction_month
	WHERE b.method_code = 'AWO' AND b.rate_month = fin_month
	GROUP BY b.area_code, b.ratio
	ORDER BY b.area_code;

-- CP hoa hồng
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('816000000001','816000000002','816000000003')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT  fin_month as month_key ,
    		17 AS id,
            b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code
          AND a.account_code IN ('816000000001','816000000002','816000000003')
          AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'AWO' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;

-- CP thuần KD khác
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('809000000002','809000000001','811000000001','811000000102','811000000002',
    '811014000001', '811037000001','811039000001','811041000001','815000000001','819000000002','819000000003','819000000001','790000000003',
    '790000050101','790000000101','790037000001','849000000001','899000000003','899000000002','811000000101','819000060001')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT  fin_month as month_key ,
    		18 AS id,
            b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code
          AND a.account_code IN ('809000000002','809000000001','811000000001','811000000102','811000000002',
    '811014000001', '811037000001','811039000001','811041000001','815000000001','819000000002','819000000003','819000000001','790000000003',
    '790000050101','790000000101','790037000001','849000000001','899000000003','899000000002','811000000101','819000060001')
          AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'AWO' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;

-- DT kinh doanh
    SELECT SUM(amount)
    INTO f_total
    FROM fact_txn_month_data
    WHERE account_code IN ('702000010001','702000010002','704000000001','705000000001','709000000001','714000000002','714000000003',
    					'714037000001','714000000004','714014000001','715000000001','715037000001','719000000001','709000000101','719000000101')
      AND transaction_date <= transaction_month
      AND area_code = '00';

    INSERT INTO report_finance_table (month_key,id, area_code, amount)
    SELECT  fin_month as month_key ,
    		16 AS id,
            b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
    FROM rate_allocation_area_month b
    LEFT JOIN fact_txn_month_data a
           ON a.area_code = b.area_code
          AND a.account_code IN ('702000010001','702000010002','704000000001','705000000001','709000000001','714000000002','714000000003',
    					'714037000001','714000000004','714014000001','715000000001','715037000001','719000000001','709000000101','719000000101')
          AND a.transaction_date <= transaction_month
    WHERE b.method_code = 'AWO' AND b.rate_month = fin_month
    GROUP BY b.area_code, b.ratio
    ORDER BY b.area_code;
   
	   -- CP hợp tác kd tàu (net)
	SELECT SUM(amount)
	INTO f_total
	FROM fact_txn_month_data
	WHERE account_code IN ('<nếu có GL code>')  -- hiện tại chưa có, sẽ để trống
	  AND transaction_date <= transaction_month
	  AND area_code = '00';
	
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT 	fin_month as month_key ,
			19 AS id,
	        b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
	FROM rate_allocation_area_month b
	LEFT JOIN fact_txn_month_data a
	       ON a.area_code = b.area_code
	      AND a.account_code IN ('<nếu có GL code>')
	      AND a.transaction_date <= transaction_month
	WHERE b.method_code = 'AWO' AND b.rate_month = fin_month
	GROUP BY b.area_code, b.ratio
	ORDER BY b.area_code;

-- CP thuế, phí
	SELECT SUM(amount)
	INTO f_total
	FROM fact_txn_month_data
	WHERE account_code IN ('831000000001','831000000002','832000000101','832000000001','831000000102')  
	  AND transaction_date <= transaction_month
	  AND area_code = '00';
	
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			22 AS id,
	        b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
	FROM rate_allocation_area_month b
	LEFT JOIN fact_txn_month_data a
	       ON a.area_code = b.area_code
	      AND a.account_code IN ('831000000001','831000000002','832000000101','832000000001','831000000102')
	      AND a.transaction_date <= transaction_month
	WHERE b.method_code = 'SM_COUNT' AND b.rate_month = fin_month
	GROUP BY b.area_code, b.ratio
	ORDER BY b.area_code;

-- CP nhân viên
	SELECT SUM(amount)
	INTO f_total
	FROM fact_txn_month_data
	WHERE account_code like '85%' 
	  AND transaction_date <= transaction_month
	  AND area_code = '00';
	
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			23 AS id,
	        b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
	FROM rate_allocation_area_month b
	LEFT JOIN fact_txn_month_data a
	       ON a.area_code = b.area_code
	      AND a.account_code  like '85%'
	      AND a.transaction_date <= transaction_month
	WHERE b.method_code = 'SM_COUNT' AND b.rate_month = fin_month
	GROUP BY b.area_code, b.ratio
	ORDER BY b.area_code;

-- CP quản lý
	SELECT SUM(amount)
	INTO f_total
	FROM fact_txn_month_data
	WHERE account_code like '86%'
	  AND transaction_date <= transaction_month
	  AND area_code = '00';
	
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			24 AS id,
	        b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
	FROM rate_allocation_area_month b
	LEFT JOIN fact_txn_month_data a
	       ON a.area_code = b.area_code
	      AND a.account_code like '86%'
	      AND a.transaction_date <= transaction_month
	WHERE b.method_code = 'SM_COUNT' AND b.rate_month = fin_month
	GROUP BY b.area_code, b.ratio
	ORDER BY b.area_code;

-- CP tài sản
	SELECT SUM(amount)
	INTO f_total
	FROM fact_txn_month_data
	WHERE account_code like '87%'
	  AND transaction_date <= transaction_month
	  AND area_code = '00';
	
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			25 AS id,
	        b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0)  + COALESCE(b.ratio, 0) * f_total 
	END AS amount
	FROM rate_allocation_area_month b
	LEFT JOIN fact_txn_month_data a
	       ON a.area_code = b.area_code
	      AND a.account_code like '87%'
	      AND a.transaction_date <= transaction_month
	WHERE b.method_code = 'SM_COUNT' AND b.rate_month = fin_month
	GROUP BY b.area_code, b.ratio
	ORDER BY b.area_code;


-- Bước 3 
-- level 1 : 

-- Chi phí dự phòng : 
	SELECT SUM(amount)
	INTO f_total
	FROM fact_txn_month_data
	WHERE account_code in	( '790000050001', '882200050001', '790000030001', '882200030001', '790000000001', 
'790000020101', '882200000001', '882200050101', '882200020101', '882200060001','790000050101', '882200030101')
	  AND transaction_date <= transaction_month
	  AND area_code = '00';
	
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			26 AS id,
	        b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0)  + COALESCE(b.ratio, 0) * f_total 
	END AS amount
	FROM rate_allocation_area_month b
	LEFT JOIN fact_txn_month_data a
	       ON a.area_code = b.area_code
	      AND a.account_code in	( '790000050001', '882200050001', '790000030001', '882200030001', '790000000001', 
'790000020101', '882200000001', '882200050101', '882200020101', '882200060001','790000050101', '882200030101')
	      AND a.transaction_date <= transaction_month
	WHERE b.method_code = 'BWO_GR2_5' AND b.rate_month = fin_month
	GROUP BY b.area_code, b.ratio
	ORDER BY b.area_code;
-- Thu nhập từ hoạt động thẻ 
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			2 AS id,
	        a.area_code as area_code ,
	        sum(amount)
	from report_finance_table as a 
	left join dim_pnl_structure as b on a.id = b.pnl_id 
	where b.pnl_parent_id  =  2 AND a.month_key = fin_month  
	group by a.area_code ;
-- Chi phí thuần kinh doanh vốn 
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			8 AS id,
	        a.area_code as area_code ,
	        sum(amount)
	from report_finance_table as a 
	left join dim_pnl_structure as b on a.id = b.pnl_id 
	where b.pnl_parent_id  =  8  AND a.month_key = fin_month
	group by a.area_code ;
-- Chi phí hoạt động khác 
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			13 AS id,
	        a.area_code as area_code ,
	        sum(amount)
	from report_finance_table as a 
	left join dim_pnl_structure as b on a.id = b.pnl_id 
	where b.pnl_parent_id  =  13 AND a.month_key = fin_month
	group by a.area_code ;
-- Tổng thu nhập các hoạt động 
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			20 AS id,
	        a.area_code as area_code ,
	        sum(amount)
	from report_finance_table as a 
	left join dim_pnl_structure as b on a.id = b.pnl_id 
	where b.pnl_parent_id  =  20 AND a.month_key = fin_month
	group by a.area_code ;
-- Tổng chi phí các hoạt động 
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			21 AS id,
	        a.area_code as area_code ,
	        sum(amount)
	from report_finance_table as a 
	left join dim_pnl_structure as b on a.id = b.pnl_id 
	where b.pnl_parent_id  =  21  AND a.month_key = fin_month
	group by a.area_code ;
-- Số lượng nhân sự 
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			27 AS id,
			b.area_code as area_code ,
			count(distinct sale_name) as amount 
	from fact_kpi_asm as a
	left join dim_area_table as b on a.area_name = b.area_name  
	WHERE a.month_col = fin_month % 100 and a.loan_to_new is not null 
	group by b.area_code 
	UNION ALL
				-- Thêm Head = tổng tất cả khu vực
	SELECT  fin_month as month_key ,
			27 AS id,
	       'A' AS area_code,
	       COUNT(DISTINCT a.sale_name) AS amount
	FROM fact_kpi_asm AS a
	LEFT JOIN dim_area_table AS b 
	       ON a.area_name = b.area_name  
	WHERE a.month_col = fin_month % 100  AND a.loan_to_new IS NOT NULL;

-- Bước 4 
-- Lợi nhuận trước thuế 
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			1 AS id,
	        a.area_code as area_code ,
	        sum(amount)
	from report_finance_table as a 
	left join dim_pnl_structure as b on a.id = b.pnl_id 
	where b.pnl_parent_id  =  1 AND a.month_key = fin_month
	group by a.area_code ;

-- Bước 5 
-- Chỉ số tài chính 
	SELECT SUM(amount)
	INTO f_total
	FROM fact_txn_month_data
	WHERE account_code IN ('<nếu có GL code>')  -- hiện tại chưa có, sẽ để trống
	  AND transaction_date <= transaction_month
	  AND area_code = '00';
	
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			28 AS id,
	        b.area_code,
	CASE 
	    WHEN b.area_code = 'A' 
	         THEN f_total   
	    ELSE COALESCE(SUM(a.amount), 0) + COALESCE(b.ratio, 0) * f_total 
	END AS amount
	FROM rate_allocation_area_month b
	LEFT JOIN fact_txn_month_data a
	       ON a.area_code = b.area_code
	      AND a.account_code IN ('<nếu có GL code>')
	      AND a.transaction_date <= transaction_month
	WHERE b.method_code = 'AWO' AND b.rate_month = fin_month 
	GROUP BY b.area_code, b.ratio
	ORDER BY b.area_code;
-- CIR (%)
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			29 AS id,
			a.area_code,
	    SUM(CASE WHEN a.id = 21 THEN a.amount ELSE 0 END) :: numeric 
	      / NULLIF(SUM(CASE WHEN a.id = 20 THEN a.amount ELSE 0 END)::numeric, 0) *100
	      AS ratio_income_cost
	FROM report_finance_table a
	WHERE a.id IN (20,21) AND a.month_key = fin_month
	GROUP BY a.area_code
	ORDER BY a.area_code;
-- Margin (%) = Lợi nhuận trước thuế(id=1) / tổng doanh thu (id=9,2,14,15,16)
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			30 AS id,
			a.area_code,
	    SUM(CASE WHEN a.id = 1 THEN a.amount ELSE 0 END) :: numeric 
	      / NULLIF(SUM(CASE WHEN a.id in (2,9,14,15,16) THEN a.amount ELSE 0 END)::numeric, 0) *100
	      AS ratio_income_cost
	FROM report_finance_table a
	WHERE a.id IN (1,2,9,14,15,16) AND a.month_key = fin_month
	GROUP BY a.area_code
	ORDER BY a.area_code;
-- Hiệu suất trên/vốn (%) = Lợi nhuân trước thuế (id =1 ) / Chi phí KDV (id = 8)
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			31 AS id,
			a.area_code,
	    SUM(CASE WHEN a.id = 1 THEN a.amount ELSE 0 END) :: numeric 
	      / NULLIF(SUM(CASE WHEN a.id = 8 THEN a.amount ELSE 0 END)::numeric, 0) *100
	      AS ratio_income_cost
	FROM report_finance_table a
	WHERE a.id IN (1,8) AND a.month_key = fin_month
	GROUP BY a.area_code
	ORDER BY a.area_code;
-- Hiệu suất BQ/ Nhân sự = Lợi nhuận trước thuế (id =1 )/ Số lượng SM (id = 27)
	INSERT INTO report_finance_table (month_key,id, area_code, amount)
	SELECT  fin_month as month_key ,
			32 AS id,
			a.area_code,
	    SUM(CASE WHEN a.id = 1 THEN a.amount ELSE 0 END) :: numeric 
	      / NULLIF(SUM(CASE WHEN a.id = 27 THEN a.amount ELSE 0 END)::numeric, 0)
	      AS ratio_income_cost
	FROM report_finance_table a
	WHERE a.id IN (1,27) AND a.month_key = fin_month
	GROUP BY a.area_code
	ORDER BY a.area_code;


-- Bước 6 : Kết thúc procedure 
END;
$$;





--Tạo Procedure để báo cáo về tình hình nhân sự ASM 
CREATE OR REPLACE PROCEDURE report_asm (asm_month int4)
AS $$
BEGIN 
-- ---------------------
    -- THÔNG TIN NGƯỜI TẠO
    -- ---------------------
    -- Tên người tạo: Mai Quang Dũng
    -- Ngày tạo: 25-08-2025

    -- ---------------------
    -- THÔNG TIN NGƯỜI CẬP NHẬT
    -- ---------------------
    -- Tên người cập nhật: Ten_Nguoi_Cap_Nhat
    -- Ngày cập nhật: current_timestamp
    -- Mục đích cập nhật: Mô tả mục đích sửa đổi, nâng cấp, hoặc sửa lỗi
    -- ---------------------
	
    -- SUMMARY LUỒNG XỬ LÝ
    -- ---------------------
    -- Bước 1: Chuẩn bị dữ liệu gốc cho từng ASM
    --         * LTN bình quân (loan_to_new).
    --         * PSDN bình quân.
    --         * Approval rate bình quân.
    --         * NPL trước WO lũy kế (từ dữ liệu NPL sau WO, tổng dư nợ sau WO và WO lũy kế).
    --
    -- Bước 2: Chuẩn bị dữ liệu chỉ tiêu tài chính cho từng ASM
    --         * CIR (%).
    --         * Margin (%).
    --         * Hiệu suất vốn.
    --         * Hiệu suất bình quân / Nhân sự.
    --
    -- Bước 3: Tạo bảng tổng hợp tmp_asm_table
    --         * Ghép tất cả các tiêu chí (8 chỉ tiêu trên) theo ASM.
    --         * Với mỗi chỉ tiêu:
    --             - Tính giá trị trung bình.
    --             - Xếp hạng (rank) ASM theo tiêu chí đó.(sử dụng dense_rank , rank(), row_number )
    --
    -- Bước 4: Sinh bảng kết quả report_asm_final_table
    --         * Tính tổng điểm = tổng các rank của 8 tiêu chí.
    --         * Tính rank tổng (rank_final) dựa trên tổng điểm.
    --         * Ghi nhận chi tiết cho từng ASM:
    --             - Giá trị của từng tiêu chí.
    --             - Rank từng tiêu chí.
    --             - Tổng điểm và rank tổng.
    --
    -- Bước 5: Kết thúc procedure
    --         * Bảng kết quả cuối: report_asm_final_table,
    --           phục vụ báo cáo và đánh giá hiệu quả ASM.
	
    -- ---------------------
    -- CHI TIẾT CÁC BƯỚC
    -- --------------------------------------------------------------------------
    -- Xoá dữ liệu cũ của tháng cần chạy
    	DELETE FROM report_asm_final_table  WHERE month_key = asm_month;
-- Bước 1 
    -- LTN bình quân
    DROP TABLE IF EXISTS tmp_ltn_avg_table;
    CREATE TABLE tmp_ltn_avg_table AS 
        SELECT asm_month as month_key, area_name, sale_name, email, AVG(loan_to_new) AS ltn_avg 
        FROM fact_kpi_asm
        WHERE month_col <= asm_month % 100 AND loan_to_new IS NOT NULL   
        GROUP BY area_name, sale_name, email;

    -- PSDN bình quân
    DROP TABLE IF EXISTS tmp_psdn_avg_table;
    CREATE TABLE tmp_psdn_avg_table AS 
        SELECT asm_month as month_key,area_name, sale_name, email, AVG(psdn) AS psdn_avg 
        FROM fact_kpi_asm
        WHERE month_col <= asm_month % 100 AND psdn IS NOT NULL   
        GROUP BY area_name, sale_name, email;

    -- Approval rate bình quân
    DROP TABLE IF EXISTS tmp_approval_rate_table;
    CREATE TABLE tmp_approval_rate_table AS 
        SELECT asm_month as month_key,area_name, sale_name, email, AVG(approval_rate) AS approval_rate_avg
        FROM fact_kpi_asm
        WHERE month_col <= asm_month % 100 AND approval_rate IS NOT NULL   
        GROUP BY area_name, sale_name, email;

  DROP TABLE IF EXISTS tmp_npl_sau_wo;
 	-- npl sau wo 
		CREATE TABLE tmp_npl_sau_wo AS
		SELECT asm_month as month_key,b.area_code as area_code ,
		       SUM(COALESCE(a.outstanding_principal,0)) AS npl_sau_wo
		FROM fact_kpi_month_raw_data a
		JOIN dim_province_table b ON a.pos_city = b.province_name
		WHERE a.kpi_month = asm_month 
		  AND a.max_bucket IN (3,4,5)
		GROUP BY b.area_code;

	-- total sau wo
		DROP TABLE IF EXISTS tmp_total_sau_wo;
		CREATE TABLE tmp_total_sau_wo AS
		SELECT asm_month as month_key,b.area_code as area_code ,
		       SUM(COALESCE(a.outstanding_principal,0)) AS tong_sau_wo
		FROM fact_kpi_month_raw_data a
		JOIN dim_province_table b ON a.pos_city = b.province_name
		WHERE a.kpi_month = asm_month
		GROUP BY b.area_code;
	-- WO luy ke 
		DROP TABLE IF EXISTS tmp_wo_luyke;
		CREATE TABLE tmp_wo_luyke AS
		SELECT y.month_key, y.area_code as area_code ,
		       SUM(x.total_wo_bal_current) AS wo_luyke
		FROM (
		    SELECT a.kpi_month AS month_key, b.area_code,
		           SUM(write_off_balance_principal) AS total_wo_bal_current
		    FROM fact_kpi_month_raw_data a
		    JOIN dim_province_table b ON a.pos_city = b.province_name
		    WHERE a.write_off_month = a.kpi_month
		    GROUP BY a.kpi_month, b.area_code
		) x
		JOIN (
		    SELECT a.kpi_month AS month_key, b.area_code,
		           SUM(write_off_balance_principal) AS total_wo_bal_current
		    FROM fact_kpi_month_raw_data a
		    JOIN dim_province_table b ON a.pos_city = b.province_name
		    WHERE a.write_off_month = a.kpi_month
		    GROUP BY a.kpi_month, b.area_code
		) y 
		  ON x.area_code = y.area_code 
		 AND x.month_key <= y.month_key
		WHERE y.month_key = asm_month 
		GROUP BY y.month_key, y.area_code;
		-- NPL trước WO lũy kế theo từng khu vực 
		DROP TABLE IF EXISTS npl_truoc_wo_luy_ke_by_area;
		CREATE TABLE npl_truoc_wo_luy_ke_by_area AS
		SELECT 
		    w.area_code,
		    (COALESCE(n.npl_sau_wo, 0) + COALESCE(w.wo_luyke, 0)) * 100.0
		    / NULLIF(COALESCE(t.tong_sau_wo, 0) + COALESCE(w.wo_luyke, 0), 0) 
		        AS npl_truoc_wo_luy_ke
		FROM tmp_npl_sau_wo   AS n
		JOIN tmp_total_sau_wo AS t ON n.area_code = t.area_code
		JOIN tmp_wo_luyke     AS w ON t.area_code = w.area_code;
		
		-- NPL trước WO theo từng ASM (SM)
		DROP TABLE IF EXISTS npl_truoc_wo_luy_ke;
		CREATE TABLE npl_truoc_wo_luy_ke AS
		SELECT 
			asm_month as month_key,
		    a.area_name, 
		    a.sale_name, 
		    a.email,  
		    c.npl_truoc_wo_luy_ke
		FROM fact_kpi_asm AS a
		LEFT JOIN dim_area_table AS b 
		       ON a.area_name = b.area_name 
		LEFT JOIN npl_truoc_wo_luy_ke_by_area AS c 
		       ON b.area_code = c.area_code 
		WHERE a.month_col = asm_month % 100  
		  AND a.approval_rate IS NOT NULL;

-- Bước 2 
		 
    -- CIR
    DROP TABLE IF EXISTS tmp_cir_table;
    CREATE TABLE tmp_cir_table AS 
        SELECT asm_month as month_key,a.area_name, a.sale_name, a.email, c.amount AS cir
        FROM fact_kpi_asm a
        LEFT JOIN dim_area_table b ON a.area_name = b.area_name   
        LEFT JOIN report_finance_table c ON b.area_code = c.area_code  
        WHERE a.month_col <= asm_month % 100
          AND a.approval_rate IS NOT NULL
          AND c.id = 29 AND c.month_key = asm_month
        GROUP BY a.area_name, a.sale_name, a.email, c.amount;

    -- Margin
    DROP TABLE IF EXISTS tmp_margin_table;
    CREATE TABLE tmp_margin_table AS 
        SELECT asm_month as month_key,a.area_name, a.sale_name, a.email, c.amount AS margin
        FROM fact_kpi_asm a
        LEFT JOIN dim_area_table b ON a.area_name = b.area_name   
        LEFT JOIN report_finance_table c ON b.area_code = c.area_code  
        WHERE a.month_col <= asm_month % 100
          AND a.approval_rate IS NOT NULL
          AND c.id = 30 AND c.month_key = asm_month
        GROUP BY a.area_name, a.sale_name, a.email, c.amount;

    -- Hiệu suất vốn
    DROP TABLE IF EXISTS tmp_hs_von_table;
    CREATE TABLE tmp_hs_von_table AS 
        SELECT asm_month as month_key,a.area_name, a.sale_name, a.email, c.amount AS hs_von
        FROM fact_kpi_asm a
        LEFT JOIN dim_area_table b ON a.area_name = b.area_name   
        LEFT JOIN report_finance_table c ON b.area_code = c.area_code  
        WHERE a.month_col <= asm_month % 100
          AND a.approval_rate IS NOT NULL
          AND c.id = 31 AND c.month_key = asm_month
        GROUP BY a.area_name, a.sale_name, a.email, c.amount;

    -- Hiệu suất BQ/Nhân sự
    DROP TABLE IF EXISTS tmp_hsbp_nhan_su_table;
    CREATE TABLE tmp_hsbp_nhan_su_table AS 
        SELECT asm_month as month_key,a.area_name, a.sale_name, a.email, c.amount AS hsbp_nhan_su
        FROM fact_kpi_asm a
        LEFT JOIN dim_area_table b ON a.area_name = b.area_name   
        LEFT JOIN report_finance_table c ON b.area_code = c.area_code  
        WHERE a.month_col <= asm_month % 100
          AND a.approval_rate IS NOT NULL
          AND c.id = 32 AND c.month_key = asm_month
        GROUP BY a.area_name, a.sale_name, a.email, c.amount;

-- Bước 3 
	DROP TABLE IF EXISTS tmp_asm_table;
	CREATE TABLE tmp_asm_table AS
	SELECT 
	    asm_month as month_key,
	    l.email , l.area_name , l.sale_name,
	    l.ltn_avg, p.psdn_avg , ar.approval_rate_avg, n.npl_truoc_wo_luy_ke,
	    c.cir , m.margin , hv.hs_von , h.hsbp_nhan_su,
	
	    -- Rank Quy mô
	    ROW_NUMBER() OVER (ORDER BY l.ltn_avg DESC)            AS rank_ltn,
	    ROW_NUMBER() OVER (ORDER BY p.psdn_avg DESC)           AS rank_psdn,
	    ROW_NUMBER() OVER (ORDER BY ar.approval_rate_avg DESC) AS rank_approval,
	    RANK()       OVER (ORDER BY n.npl_truoc_wo_luy_ke ASC) AS rank_npl,
	
	    -- Rank Tài chính
	    DENSE_RANK() OVER (ORDER BY c.cir DESC)     AS rank_cir,
	    DENSE_RANK() OVER (ORDER BY m.margin DESC)  AS rank_margin,
	    DENSE_RANK() OVER (ORDER BY hv.hs_von ASC)  AS rank_hs_von,
	    DENSE_RANK() OVER (ORDER BY h.hsbp_nhan_su DESC) AS rank_hsbp
	
	FROM tmp_ltn_avg_table l
	LEFT JOIN tmp_psdn_avg_table p         ON l.email = p.email  AND l.month_key = p.month_key
	LEFT JOIN tmp_approval_rate_table ar   ON l.email = ar.email AND l.month_key = ar.month_key
	LEFT JOIN npl_truoc_wo_luy_ke n        ON l.email = n.email  AND l.month_key = n.month_key
	LEFT JOIN tmp_cir_table c              ON l.email = c.email  AND l.month_key = c.month_key
	LEFT JOIN tmp_margin_table m           ON l.email = m.email  AND l.month_key = m.month_key
	LEFT JOIN tmp_hs_von_table hv          ON l.email = hv.email AND l.month_key = hv.month_key
	LEFT JOIN tmp_hsbp_nhan_su_table h     ON l.email = h.email  AND l.month_key = h.month_key;

-- Bước 4    

-- Ghi dữ liệu mới
INSERT INTO report_asm_final_table (
    month_key, area_name, email,
    tong_diem, rank_final,
    ltn_avg, rank_ltn_avg,
    psdn_avg, rank_psdn_avg,
    approval_rate_avg, rank_approval_rate_avg,
    npl_truoc_wo_luy_ke, rank_npl_truoc_wo_luy_ke,
    diem_quymo, rank_ptkd,
    cir, rank_cir,
    margin, rank_margin,
    hs_von, rank_hs_von,
    hsbp_nhan_su, rank_hsbq_nhan_su,
    diem_fin, rank_fin
)
SELECT
    month_key, area_name, email,
    (rank_ltn + rank_psdn + rank_approval + rank_npl
     + rank_cir + rank_margin + rank_hs_von + rank_hsbp) AS tong_diem,
     
    ROW_NUMBER() OVER (ORDER BY (rank_ltn + rank_psdn + rank_approval + rank_npl
                               + rank_cir + rank_margin + rank_hs_von + rank_hsbp) ASC) AS rank_final,
    ltn_avg, rank_ltn AS rank_ltn_avg,
    psdn_avg, rank_psdn AS rank_psdn_avg,
    approval_rate_avg, rank_approval AS rank_approval_rate_avg,
    npl_truoc_wo_luy_ke, rank_npl AS rank_npl_truoc_wo_luy_ke,
    (rank_ltn + rank_psdn + rank_approval + rank_npl) AS diem_quymo,
    
    ROW_NUMBER() OVER (ORDER BY (rank_ltn + rank_psdn + rank_approval + rank_npl) ASC) AS rank_ptkd,
    cir, rank_cir,
    margin, rank_margin,
    hs_von, rank_hs_von,
    hsbp_nhan_su, rank_hsbp AS rank_hsbq_nhan_su,
    (rank_cir + rank_margin + rank_hs_von + rank_hsbp) AS diem_fin,
    
    RANK() OVER (ORDER BY (rank_cir + rank_margin + rank_hs_von + rank_hsbp) ASC) AS rank_fin
FROM tmp_asm_table
WHERE month_key = asm_month;


-- Bước 5 : Kết thúc procedure 
END;
$$ LANGUAGE plpgsql;





-- Tạo ra procedure chính để gọi các 3 procedure ở trên 
CREATE OR REPLACE PROCEDURE all_report(p_month INT4)
LANGUAGE plpgsql
AS $$
begin
-- ---------------------
    -- THÔNG TIN NGƯỜI TẠO
    -- ---------------------
    -- Tên người tạo: Mai Quang Dũng
    -- Ngày tạo: 25-08-2025

    -- ---------------------
    -- THÔNG TIN NGƯỜI CẬP NHẬT
    -- ---------------------
    -- Tên người cập nhật: Ten_Nguoi_Cap_Nhat
    -- Ngày cập nhật: current_timestamp
    -- Mục đích cập nhật: Mô tả mục đích sửa đổi, nâng cấp, hoặc sửa lỗi
    -- ---------------------
	
    -- SUMMARY LUỒNG XỬ LÝ
    -- ---------------------
    -- Bước 1: Gọi procedure rate_area_month
    --         * Tính toán và phân bổ tỷ lệ theo khu vực theo tháng p_month.
    --         * Nếu lỗi, ghi log vào error_log (proc_name = 'rate_area_month').
    --
    -- Bước 2: Gọi procedure report_finance
    --         * Tính toán các chỉ tiêu báo cáo tài chính theo khu vực cho tháng p_month.
    --         * Nếu lỗi, ghi log vào error_log (proc_name = 'report_finance').
    --
    -- Bước 3: Gọi procedure report_asm
    --         * Tính toán và xếp hạng hiệu quả ASM theo khu vực/tháng p_month.
    --         * Nếu lỗi, ghi log vào error_log (proc_name = 'report_asm').
    --
    -- Lưu ý:
    --   * Các bước chạy độc lập. Nếu một bước lỗi thì vẫn tiếp tục bước sau.
    --   * error_log chỉ ghi lại những bước gặp lỗi (best-effort run).
    --   * Kết quả cuối cùng gồm:
    --       - Bảng rate_allocation_area_month (tỷ lệ phân bổ).
    --       - Bảng report_finance_table (báo cáo tài chính).
    --       - Bảng report_asm_final_table (xếp hạng ASM).
	
    -- ---------------------
    -- CHI TIẾT CÁC BƯỚC
    -- --------------------------------------------------------------------------

    -- Bước 1  
    BEGIN
        CALL rate_area_month(p_month);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO error_log(proc_name, run_month, error_message,end_time)
        VALUES ('rate_area_month', p_month, SQLERRM, now());
    END;

    -- Bước 2 
    BEGIN
        CALL report_finance(p_month);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO error_log(proc_name, run_month, error_message,end_time)
        VALUES ('report_finance', p_month, SQLERRM, now());
    END;

    -- Bước 3 
    BEGIN
        CALL report_asm(p_month);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO error_log(proc_name, run_month, error_message,end_time)
        VALUES ('report_asm', p_month, SQLERRM, now());
    END;
END;
$$;


