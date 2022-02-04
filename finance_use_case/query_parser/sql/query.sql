WITH trans AS ( SELECT account_id, date, type, operation, amount, balance, k_symbol, bank, account, trans_id FROM `id-bi-staging.playground_dev.public_dataset_financial_trans` WHERE date >= '1998-12-01'), account AS ( SELECT district_id, frequency, date, account_id FROM `id-bi-staging.playground_dev.public_dataset_financial_account`), district AS ( SELECT A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, district_id FROM `id-bi-staging.playground_dev.public_dataset_financial_district`), disp AS ( SELECT client_id, account_id, type, disp_id FROM `id-bi-staging.playground_dev.public_dataset_financial_disp`), client AS ( SELECT gender, birth_date, district_id, client_id FROM `id-bi-staging.playground_dev.public_dataset_financial_client`) SELECT trans.date, trans.type AS trans_type, trans.operation, trans.amount, trans.balance, trans.k_symbol, trans.bank, trans.account, account.frequency, account.account_id, district.A2, district.A3, district.A4, district.A5, district.A6, district.A7, district.A8, district.A9, district.A10, district.A11, district.A12, district.A13, district.A14, district.A15, district.A16, district.district_id, disp.type, disp.type as disp_type, client.gender, client.birth_date FROM trans JOIN account ON account.account_id = trans.account_id JOIN district ON district.district_id = account.district_id JOIN disp ON account.account_id = disp.account_id JOIN client ON client.client_id = disp.client_id