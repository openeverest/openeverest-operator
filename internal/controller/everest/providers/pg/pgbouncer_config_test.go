package pg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePGBouncerConfig(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		check   func(*testing.T, *struct{ Global, Databases, Users map[string]string })
		wantErr bool
	}{
		{
			name: "empty",
			yaml: "",
			check: func(t *testing.T, c *struct {
				Global    map[string]string
				Databases map[string]string
				Users     map[string]string
			}) {
				assert.Nil(t, c.Global)
				assert.Nil(t, c.Databases)
				assert.Nil(t, c.Users)
			},
		},
		{
			name: "whitespace only",
			yaml: "  \n\t  ",
			check: func(t *testing.T, c *struct {
				Global    map[string]string
				Databases map[string]string
				Users     map[string]string
			}) {
				assert.Nil(t, c.Global)
			},
		},
		{
			name: "global only",
			yaml: `
global:
  client_tls_sslmode: allow
  default_pool_size: "150"
  max_client_conn: "300"
`,
			check: func(t *testing.T, c *struct {
				Global    map[string]string
				Databases map[string]string
				Users     map[string]string
			}) {
				require.NotNil(t, c.Global)
				assert.Equal(t, "allow", c.Global["client_tls_sslmode"])
				assert.Equal(t, "150", c.Global["default_pool_size"])
				assert.Equal(t, "300", c.Global["max_client_conn"])
				assert.Nil(t, c.Databases)
				assert.Nil(t, c.Users)
			},
		},
		{
			name: "global with numeric values",
			yaml: `
global:
  default_pool_size: 150
  max_client_conn: 300
`,
			check: func(t *testing.T, c *struct {
				Global    map[string]string
				Databases map[string]string
				Users     map[string]string
			}) {
				require.NotNil(t, c.Global)
				assert.Equal(t, "150", c.Global["default_pool_size"])
				assert.Equal(t, "300", c.Global["max_client_conn"])
			},
		},
		{
			name:    "invalid YAML",
			yaml:    "global:\n  [",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePGBouncerConfig(tt.yaml)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, &struct {
					Global    map[string]string
					Databases map[string]string
					Users     map[string]string
				}{got.Global, got.Databases, got.Users})
			}
		})
	}
}
