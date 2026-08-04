ALTER TABLE installation_budgets
    DROP CONSTRAINT installation_budgets_class_check;

ALTER TABLE installation_budgets
    ADD CONSTRAINT installation_budgets_class_check
    CHECK (class = ANY (ARRAY['rest', 'app_jwt_rest', 'graphql']));
