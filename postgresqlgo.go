package postgresqlgo

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var poolx *pgxpool.Pool

type Postgresqlgo struct {
	Files fs.FS
}

// Conn obtiene una conexión a postgres desde el pool
func (p *Postgresqlgo) Conn() *pgxpool.Conn {
	conn, err := p.ConnWithErr()
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	return conn
}

// ConnWithErr obtiene una conexión a postgres desde el pool o un error
// utiliza las variables de ambiente: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_DB y POSTGRES_PASSWORD
func (p *Postgresqlgo) ConnWithErr() (*pgxpool.Conn, error) {
	config := pgx.ConnConfig{}
	port, _ := strconv.ParseUint(os.Getenv("POSTGRES_PORT"), 10, 16)
	config.Port = uint16(port)
	config.Host = os.Getenv("POSTGRES_HOST")
	config.User = os.Getenv("POSTGRES_USER")
	config.Database = os.Getenv("POSTGRES_DB")
	config.Password = os.Getenv("POSTGRES_PASSWORD")
	postgresurl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", config.User, config.Password, config.Host, config.Port, config.Database)
	if poolx == nil {
		log.Println("creando pool", postgresurl)
		cfg, err := pgxpool.ParseConfig(postgresurl)
		cfg.MaxConns = 20
		cfg.MinConns = 2
		if err != nil {
			log.Println(err)
		}
		pl, err := pgxpool.NewWithConfig(context.Background(), cfg)
		if err != nil {
			log.Println("error al crear pool", err)
			return nil, err
		}
		poolx = pl
		log.Println("pool creado")
	}
	conn, err := poolx.Acquire(context.Background())
	if err != nil {
		log.Println("error al obtener desde pool", err)
		return nil, err
	}
	return conn, nil
}

// ReadQuery lee el contenido de una query para poder utilizarla posteriormente
func (p *Postgresqlgo) ReadQuery(sql string) string {
	file, err := p.Files.Open(sql)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	bytes, _ := io.ReadAll(file)
	query := string(bytes)
	return query
}
