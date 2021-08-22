package broker

import "time"

// ClientSubscription entry detailing a client subscription session
type ClientSubscription struct {
	ClientName     string    `json:"client_name" validate:"required"`
	ServingNode    string    `json:"serving_node" validate:"required"`
	StatusUpdateAt time.Time `json:"updated_at"`
	EstablishedAt  time.Time `json:"established_at"`
}

// SubscriptionRecords record of subscriptions active at the moment
type SubscriptionRecords struct {
	ActiveSessions map[string]ClientSubscription `json:"active_sessions" validate:"required,dive"`
}

// ========================================================================================
// Controller operating the active subscription records

// SubscriptionRecorder manage the active subscription records
type SubscriptionRecorder interface {
}

// type subscriptionRecorderImpl struct {
// }
