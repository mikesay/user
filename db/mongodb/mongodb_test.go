package mongodb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mikesay/user/users"
	"go.mongodb.org/mongo-driver/bson/primitive" // New BSON package
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	TestMongo Mongo
	TestUser  = users.User{
		FirstName: "firstname",
		LastName:  "lastname",
		Username:  "username",
		Password:  "blahblah",
		Addresses: []users.Address{{Street: "street"}},
	}
)

func TestMain(m *testing.M) {
	// Setup: Connect to a real local mongo or a container
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		fmt.Printf("Failed to connect to Mongo: %v\n", err)
		os.Exit(1)
	}

	TestMongo.Client = client
	TestMongo.Database = client.Database("test_users")

	// Replace EnsureIndexes with your new implementation
	TestMongo.EnsureIndexes()

	code := m.Run()

	// Teardown
	TestMongo.Database.Drop(context.Background())
	client.Disconnect(context.Background())
	os.Exit(code)
}

func TestAddUserIDs(t *testing.T) {
	// Refactor mgo.ObjectId to primitive.ObjectID
	uid := primitive.NewObjectID()
	cid := primitive.NewObjectID()
	aid := primitive.NewObjectID()

	// Logic remains similar, but ensure types match primitive.ObjectID
	m := New()
	m.ID = uid
	m.AddressIDs = append(m.AddressIDs, aid)
	m.CardIDs = append(m.CardIDs, cid)
	m.AddUserIDs()

	if m.UserID != uid.Hex() {
		t.Errorf("Expected %s, got %s", uid.Hex(), m.UserID)
	}
}

func TestCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := TestMongo.CreateUser(&TestUser)
	if err != nil {
		t.Error(err)
	}

	// Test duplicate
	err = TestMongo.CreateUser(&TestUser)
	if err == nil {
		t.Error("Expected duplicate key error")
	}
}

func TestGetUserByName(t *testing.T) {
	u, err := TestMongo.GetUserByName(TestUser.Username)
	if err != nil {
		t.Fatal(err)
	}
	if u.Username != TestUser.Username {
		t.Errorf("Expected %s, got %s", TestUser.Username, u.Username)
	}
}

func TestGetUser(t *testing.T) {
	// Reusing the global TestMongo.Client initialized in TestMain
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Ensure your Mongo struct method 'GetUser' is updated to accept context
	_, err := TestMongo.GetUser(TestUser.UserID)
	if err != nil {
		t.Error(err)
	}
}

func TestGetUserAttributes(t *testing.T) {
	// No session copying needed; just use the global client
	ctx := context.Background()
	_ = ctx // Use this context for any DB calls here
}

func TestGetURL(t *testing.T) {
	// This function logic is independent of the driver version
	// but ensure the returned URL matches standard MongoDB URI format
	name = "test"
	password = "password"
	host = "thishostshouldnotexist:3038"
	u := getURL()

	expected := "mongodb://test:password@thishostshouldnotexist:3038/users"
	if u.String() != expected {
		t.Errorf("expected %s, got %s", expected, u.String())
	}
}

func TestPing(t *testing.T) {
	// The official driver uses Ping(ctx, readpref)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Use the official Ping method on the Client
	err := TestMongo.Client.Ping(ctx, nil) // passing nil defaults to Primary
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}
