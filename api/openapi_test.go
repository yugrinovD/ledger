package api_test

import (
	"context"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestOpenAPIValid(t *testing.T) {
	for _, file := range []string{"openapi.yaml", "admin.openapi.yaml"} {
		t.Run(file, func(t *testing.T) {
			loader := openapi3.NewLoader()
			loader.IsExternalRefsAllowed = true
			doc, err := loader.LoadFromFile(file)
			if err != nil {
				t.Fatalf("load OpenAPI: %v", err)
			}
			if err := doc.Validate(context.Background()); err != nil {
				t.Fatalf("validate OpenAPI: %v", err)
			}
			if doc.Paths == nil || doc.Paths.Len() == 0 {
				t.Fatal("OpenAPI document has no paths")
			}
		})
	}
}

func TestMoneyPatternsMatchDomain(t *testing.T) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, schemaName := range []string{"CreateTransferRequest", "CreateHoldRequest"} {
		ref := doc.Components.Schemas[schemaName]
		if ref == nil || ref.Value == nil {
			t.Fatalf("missing schema %s", schemaName)
		}
		amount := ref.Value.Properties["amount"]
		if amount == nil || amount.Value == nil {
			t.Fatalf("missing amount in %s", schemaName)
		}
		for _, value := range []string{"10", "10.5", "10.50", "10.500"} {
			if err := amount.Value.VisitJSONString(value); err != nil {
				t.Errorf("%s: %q should be valid: %v", schemaName, value, err)
			}
		}
		for _, value := range []string{".5", "1.", "1,00", "-1", "1.2345"} {
			if err := amount.Value.VisitJSONString(value); err == nil {
				t.Errorf("%s: %q should be invalid", schemaName, value)
			}
		}
	}
}

func TestRequestBodiesMatchSpec(t *testing.T) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	transfer := doc.Components.Schemas["CreateTransferRequest"].Value
	validTransfer := map[string]any{
		"source_account_id":      "01a1f7b3-54f4-7ef8-b505-0cf1766e509f",
		"destination_account_id": "01a1f7b3-54fd-7744-8be4-6470171551ee",
		"amount":                 "10.50",
		"currency":               "USD",
		"description":            "contract test",
	}
	if err := transfer.VisitJSON(validTransfer); err != nil {
		t.Fatalf("valid transfer rejected: %v", err)
	}
	invalidTransfer := map[string]any{
		"source_account_id":      "01a1f7b3-54f4-7ef8-b505-0cf1766e509f",
		"destination_account_id": "01a1f7b3-54fd-7744-8be4-6470171551ee",
		"amount":                 ".5",
		"currency":               "USD",
		"unexpected":             true,
	}
	if err := transfer.VisitJSON(invalidTransfer); err == nil {
		t.Fatal("invalid transfer accepted by schema")
	}
	hold := doc.Components.Schemas["CreateHoldRequest"].Value
	if err := hold.VisitJSON(map[string]any{
		"account_id": "01a1f7b3-54f4-7ef8-b505-0cf1766e509f",
		"amount":     "5.00",
		"currency":   "USD",
	}); err != nil {
		t.Fatalf("valid hold rejected: %v", err)
	}
}
