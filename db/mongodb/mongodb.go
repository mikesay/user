package mongodb

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/mikesay/user/users"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	name            string
	password        string
	host            string
	db              = "users"
	ErrInvalidHexID = errors.New("Invalid Id Hex")
)

func init() {
	flag.StringVar(&name, "mongo-user", os.Getenv("MONGO_USER"), "Mongo user")
	flag.StringVar(&password, "mongo-password", os.Getenv("MONGO_PASS"), "Mongo password")
	flag.StringVar(&host, "mongo-host", os.Getenv("MONGO_HOST"), "Mongo host")
}

// Mongo meets the Database interface requirements
type Mongo struct {
	Client   *mongo.Client
	Database *mongo.Database
}

// Init MongoDB using the official driver
func (m *Mongo) Init() error {
	u := getURL()

	// Ensure directConnection=true for Podman/Mac standalone setups
	q := u.Query()
	q.Set("directConnection", "true")
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(u.String()))
	if err != nil {
		return err
	}

	// Verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("mongo ping failed: %w", err)
	}

	m.Client = client
	return m.EnsureIndexes()
}

// Helper for frequent context creation
func (m *Mongo) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// MongoUser is a wrapper for the users
type MongoUser struct {
	users.User `bson:",inline"`
	ID         primitive.ObjectID   `bson:"_id"`
	AddressIDs []primitive.ObjectID `bson:"addresses"`
	CardIDs    []primitive.ObjectID `bson:"cards"`
}

// New Returns a new MongoUser
func New() MongoUser {
	u := users.New()
	return MongoUser{
		User:       u,
		AddressIDs: make([]primitive.ObjectID, 0),
		CardIDs:    make([]primitive.ObjectID, 0),
	}
}

// AddUserIDs adds userID as string to user
func (mu *MongoUser) AddUserIDs() {
	if mu.User.Addresses == nil {
		mu.User.Addresses = make([]users.Address, 0)
	}
	for _, id := range mu.AddressIDs {
		mu.User.Addresses = append(mu.User.Addresses, users.Address{ID: id.Hex()})
	}
	if mu.User.Cards == nil {
		mu.User.Cards = make([]users.Card, 0)
	}
	for _, id := range mu.CardIDs {
		mu.User.Cards = append(mu.User.Cards, users.Card{ID: id.Hex()})
	}
	mu.User.UserID = mu.ID.Hex()

	if len(mu.User.Links) == 0 {
		mu.User.Links = nil
	}
}

type MongoAddress struct {
	users.Address `bson:",inline"`
	ID            primitive.ObjectID `bson:"_id"`
}

func (ma *MongoAddress) AddID() { ma.Address.ID = ma.ID.Hex() }

type MongoCard struct {
	users.Card `bson:",inline"`
	ID         primitive.ObjectID `bson:"_id"`
}

func (mc *MongoCard) AddID() { mc.Card.ID = mc.ID.Hex() }

// CreateUser Insert user to MongoDB
func (m *Mongo) CreateUser(u *users.User) error {
	ctx, cancel := m.ctx()
	defer cancel()

	mu := New()
	mu.User = *u
	mu.ID = primitive.NewObjectID()

	var carderr, addrerr error
	mu.CardIDs, carderr = m.createCards(ctx, u.Cards)
	mu.AddressIDs, addrerr = m.createAddresses(ctx, u.Addresses)

	coll := m.Client.Database(db).Collection("customers")
	opts := options.Replace().SetUpsert(true)

	_, err := coll.ReplaceOne(ctx, bson.M{"_id": mu.ID}, mu, opts)
	if err != nil {
		m.cleanAttributes(mu)
		return err
	}

	mu.User.UserID = mu.ID.Hex()
	if carderr != nil || addrerr != nil {
		return fmt.Errorf("attribute errors: %v %v", carderr, addrerr)
	}
	*u = mu.User
	return nil
}

func (m *Mongo) createCards(ctx context.Context, cs []users.Card) ([]primitive.ObjectID, error) {
	ids := make([]primitive.ObjectID, 0)
	coll := m.Client.Database(db).Collection("cards")
	opts := options.Replace().SetUpsert(true)

	for k, ca := range cs {
		id := primitive.NewObjectID()
		mc := MongoCard{Card: ca, ID: id}
		_, err := coll.ReplaceOne(ctx, bson.M{"_id": mc.ID}, mc, opts)
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
		cs[k].ID = id.Hex()
	}
	return ids, nil
}

func (m *Mongo) createAddresses(ctx context.Context, as []users.Address) ([]primitive.ObjectID, error) {
	ids := make([]primitive.ObjectID, 0)
	coll := m.Client.Database(db).Collection("addresses")
	opts := options.Replace().SetUpsert(true)

	for k, a := range as {
		id := primitive.NewObjectID()
		ma := MongoAddress{Address: a, ID: id}
		_, err := coll.ReplaceOne(ctx, bson.M{"_id": ma.ID}, ma, opts)
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
		as[k].ID = id.Hex()
	}
	return ids, nil
}

func (m *Mongo) cleanAttributes(mu MongoUser) error {
	ctx, cancel := m.ctx()
	defer cancel()

	collA := m.Client.Database(db).Collection("addresses")
	collC := m.Client.Database(db).Collection("cards")

	_, _ = collA.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": mu.AddressIDs}})
	_, _ = collC.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": mu.CardIDs}})
	return nil
}

func (m *Mongo) appendAttributeId(attr string, id primitive.ObjectID, userid string) error {
	ctx, cancel := m.ctx()
	defer cancel()

	uid, err := primitive.ObjectIDFromHex(userid)
	if err != nil {
		return err
	}

	coll := m.Client.Database(db).Collection("customers")
	_, err = coll.UpdateOne(ctx, bson.M{"_id": uid}, bson.M{"$addToSet": bson.M{attr: id}})
	return err
}

func (m *Mongo) removeAttributeId(attr string, id primitive.ObjectID, userid string) error {
	ctx, cancel := m.ctx()
	defer cancel()

	uid, err := primitive.ObjectIDFromHex(userid)
	if err != nil {
		return err
	}

	coll := m.Client.Database(db).Collection("customers")
	_, err = coll.UpdateOne(ctx, bson.M{"_id": uid}, bson.M{"$pull": bson.M{attr: id}})
	return err
}

func (m *Mongo) GetUserByName(name string) (users.User, error) {
	ctx, cancel := m.ctx()
	defer cancel()

	coll := m.Client.Database(db).Collection("customers")
	mu := New()
	err := coll.FindOne(ctx, bson.M{"username": name}).Decode(&mu)
	if err != nil {
		return users.User{}, err
	}
	mu.AddUserIDs()
	return mu.User, nil
}

func (m *Mongo) GetUser(id string) (users.User, error) {
	ctx, cancel := m.ctx()
	defer cancel()

	uid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return users.New(), ErrInvalidHexID
	}

	coll := m.Client.Database(db).Collection("customers")
	mu := New()
	err = coll.FindOne(ctx, bson.M{"_id": uid}).Decode(&mu)
	if err != nil {
		return users.User{}, err
	}
	mu.AddUserIDs()
	return mu.User, nil
}

func (m *Mongo) GetUsers() ([]users.User, error) {
	ctx, cancel := m.ctx()
	defer cancel()

	coll := m.Client.Database(db).Collection("customers")
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var mus []MongoUser
	if err = cursor.All(ctx, &mus); err != nil {
		return nil, err
	}

	us := make([]users.User, 0)
	for _, mu := range mus {
		mu.AddUserIDs()
		us = append(us, mu.User)
	}
	return us, nil
}

func (m *Mongo) GetUserAttributes(u *users.User) error {
	ctx, cancel := m.ctx()
	defer cancel()

	// Handle Addresses
	addrIds := make([]primitive.ObjectID, 0)
	for _, a := range u.Addresses {
		if !primitive.IsValidObjectID(a.ID) {
			return ErrInvalidHexID
		}
		aid, _ := primitive.ObjectIDFromHex(a.ID)
		addrIds = append(addrIds, aid)
	}

	var ma []MongoAddress
	cursorA, err := m.Client.Database(db).Collection("addresses").Find(ctx, bson.M{"_id": bson.M{"$in": addrIds}})
	if err == nil {
		cursorA.All(ctx, &ma)
		na := make([]users.Address, 0)
		for _, a := range ma {
			a.AddID()
			na = append(na, a.Address)
		}
		u.Addresses = na
	}

	// Handle Cards
	cardIds := make([]primitive.ObjectID, 0)
	for _, c := range u.Cards {
		if !primitive.IsValidObjectID(c.ID) {
			return ErrInvalidHexID
		}
		cid, _ := primitive.ObjectIDFromHex(c.ID)
		cardIds = append(cardIds, cid)
	}

	var mc []MongoCard
	cursorC, err := m.Client.Database(db).Collection("cards").Find(ctx, bson.M{"_id": bson.M{"$in": cardIds}})
	if err == nil {
		cursorC.All(ctx, &mc)
		nc := make([]users.Card, 0)
		for _, ca := range mc {
			ca.AddID()
			nc = append(nc, ca.Card)
		}
		u.Cards = nc
	}

	return nil
}

func (m *Mongo) GetCard(id string) (users.Card, error) {
	ctx, cancel := m.ctx()
	defer cancel()

	if !primitive.IsValidObjectID(id) {
		return users.Card{}, ErrInvalidHexID
	}
	cid, _ := primitive.ObjectIDFromHex(id)

	coll := m.Client.Database(db).Collection("cards")
	mc := MongoCard{}
	err := coll.FindOne(ctx, bson.M{"_id": cid}).Decode(&mc)
	if err != nil {
		return users.Card{}, err
	}
	mc.AddID()
	return mc.Card, nil
}

func (m *Mongo) GetCards() ([]users.Card, error) {
	ctx, cancel := m.ctx()
	defer cancel()

	coll := m.Client.Database(db).Collection("cards")
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var mcs []MongoCard
	if err = cursor.All(ctx, &mcs); err != nil {
		return nil, err
	}

	cs := make([]users.Card, 0)
	for _, mc := range mcs {
		mc.AddID()
		cs = append(cs, mc.Card)
	}
	return cs, nil
}

func (m *Mongo) CreateCard(ca *users.Card, userid string) error {
	ctx, cancel := m.ctx()
	defer cancel()

	if userid != "" && !primitive.IsValidObjectID(userid) {
		return ErrInvalidHexID
	}

	coll := m.Client.Database(db).Collection("cards")
	id := primitive.NewObjectID()
	mc := MongoCard{Card: *ca, ID: id}

	opts := options.Replace().SetUpsert(true)
	_, err := coll.ReplaceOne(ctx, bson.M{"_id": mc.ID}, mc, opts)
	if err != nil {
		return err
	}

	if userid != "" {
		err = m.appendAttributeId("cards", mc.ID, userid)
		if err != nil {
			return err
		}
	}
	mc.AddID()
	*ca = mc.Card
	return err
}

// GetAddress Gets an address by object Id
func (m *Mongo) GetAddress(id string) (users.Address, error) {
	ctx, cancel := m.ctx()
	defer cancel()

	if !primitive.IsValidObjectID(id) {
		return users.Address{}, ErrInvalidHexID
	}
	aid, _ := primitive.ObjectIDFromHex(id)

	coll := m.Client.Database(db).Collection("addresses")
	ma := MongoAddress{}
	err := coll.FindOne(ctx, bson.M{"_id": aid}).Decode(&ma)
	if err != nil {
		return users.Address{}, err
	}
	ma.AddID()
	return ma.Address, nil
}

// GetAddresses gets all addresses
func (m *Mongo) GetAddresses() ([]users.Address, error) {
	ctx, cancel := m.ctx()
	defer cancel()

	coll := m.Client.Database(db).Collection("addresses")
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var mas []MongoAddress
	if err = cursor.All(ctx, &mas); err != nil {
		return nil, err
	}

	as := make([]users.Address, 0)
	for _, ma := range mas {
		ma.AddID()
		as = append(as, ma.Address)
	}
	return as, nil
}

// CreateAddress Inserts Address into MongoDB
func (m *Mongo) CreateAddress(a *users.Address, userid string) error {
	ctx, cancel := m.ctx()
	defer cancel()

	if userid != "" && !primitive.IsValidObjectID(userid) {
		return ErrInvalidHexID
	}

	coll := m.Client.Database(db).Collection("addresses")
	id := primitive.NewObjectID()
	ma := MongoAddress{Address: *a, ID: id}

	opts := options.Replace().SetUpsert(true)
	_, err := coll.ReplaceOne(ctx, bson.M{"_id": ma.ID}, ma, opts)
	if err != nil {
		return err
	}

	if userid != "" {
		err = m.appendAttributeId("addresses", ma.ID, userid)
		if err != nil {
			return err
		}
	}
	ma.AddID()
	*a = ma.Address
	return nil
}

// Delete removes entities and cleans up references
func (m *Mongo) Delete(entity, id string) error {
	ctx, cancel := m.ctx()
	defer cancel()

	if !primitive.IsValidObjectID(id) {
		return ErrInvalidHexID
	}
	oid, _ := primitive.ObjectIDFromHex(id)

	if entity == "customers" {
		// Load user to find linked addresses and cards
		u, err := m.GetUser(id)
		if err != nil {
			return err
		}

		aids := make([]primitive.ObjectID, 0)
		for _, a := range u.Addresses {
			if aid, err := primitive.ObjectIDFromHex(a.ID); err == nil {
				aids = append(aids, aid)
			}
		}
		cids := make([]primitive.ObjectID, 0)
		for _, c := range u.Cards {
			if cid, err := primitive.ObjectIDFromHex(c.ID); err == nil {
				cids = append(cids, cid)
			}
		}

		// Delete linked records
		_, _ = m.Client.Database(db).Collection("addresses").DeleteMany(ctx, bson.M{"_id": bson.M{"$in": aids}})
		_, _ = m.Client.Database(db).Collection("cards").DeleteMany(ctx, bson.M{"_id": bson.M{"$in": cids}})
	} else {
		// If deleting a card/address, pull the reference from all customers
		collCust := m.Client.Database(db).Collection("customers")
		_, _ = collCust.UpdateMany(ctx, bson.M{}, bson.M{"$pull": bson.M{entity: oid}})
	}

	// Delete the actual entity
	_, err := m.Client.Database(db).Collection(entity).DeleteOne(ctx, bson.M{"_id": oid})
	return err
}

func getURL() url.URL {
	ur := url.URL{
		Scheme: "mongodb",
		Host:   host,
		Path:   db,
	}
	if name != "" {
		ur.User = url.UserPassword(name, password)
	}
	return ur
}

// EnsureIndexes refactored for modern IndexModel
func (m *Mongo) EnsureIndexes() error {
	ctx, cancel := m.ctx()
	defer cancel()

	coll := m.Client.Database(db).Collection("customers")

	indexModel := mongo.IndexModel{
		Keys: bson.D{{Key: "username", Value: 1}},
		Options: options.Index().
			SetUnique(true).
			SetBackground(true),
	}

	_, err := coll.Indexes().CreateOne(ctx, indexModel)
	return err
}

func (m *Mongo) Ping() error {
	ctx, cancel := m.ctx()
	defer cancel()
	return m.Client.Ping(ctx, nil)
}
