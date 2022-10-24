package postgresqlgo

import (
	"context"
	"io/fs"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestConnections(t *testing.T) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:10-alpine",
		ExposedPorts: []string{"5432"},
		WaitingFor:   wait.ForListeningPort("5432/tcp"),
		Env: map[string]string{
			"POSTGRES_USER":             "postgres",
			"POSTGRES_PASSWORD":         "postgres",
			"POSTGRES_DB":               "postgres",
			"POSTGRES_HOST_AUTH_METHOD": "trust",
		},
	}
	rc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Error(err)
	}
	mappedPort, _ := rc.MappedPort(ctx, "5432")
	hostIP, _ := rc.Host(ctx)
	log.Printf("%s:%s", hostIP, mappedPort.Port())
	os.Setenv("POSTGRES_HOST", hostIP)
	os.Setenv("POSTGRES_PORT", mappedPort.Port())
	os.Setenv("POSTGRES_USER", "postgres")
	os.Setenv("POSTGRES_PASSWORD", "postgres")
	os.Setenv("POSTGRES_DB", "postgres")

	t.Run("puedo hacer una query", func(t *testing.T) {
		p := Postgresqlgo{}
		con := p.Conn()
		r, err := con.Query(context.Background(), "select extract(epoch from now())::int")
		if err != nil {
			log.Fatal(err)
		}
		defer r.Close()
		var now int
		for r.Next() {
			err := r.Scan(&now)
			if err != nil {
				log.Fatal(err)
			}
		}
		assert.Greater(t, now, 1641042061)
		con.Release()
	})

	t.Run("puedo establecer conexiones", func(t *testing.T) {
		p := Postgresqlgo{}
		for a := 0; a < 10_000; a++ {
			con := p.Conn()
			con.Release()
		}
	})
}

type TestFS struct {
}

func (tfs TestFS) Open(name string) (fs.File, error) {
	f, err := os.OpenFile(name, os.O_RDONLY, 0644)
	return f, err
}

func TestReadQuery(t *testing.T) {
	tfs := TestFS{}
	p := Postgresqlgo{Files: tfs}
	lee := p.ReadQuery("./testdata/a.sql")
	assert.Equal(t, "b", lee)
}
