package dispatch

import (
	"encoding/json"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/acme/frontier/internal/queue"
)

type recordedDelivery struct {
	GUID    string          `json:"guid"`
	Event   string          `json:"event"`
	Payload json.RawMessage `json:"payload"`
}

// This is a synthetic recording of the S-142 traffic shape. Real enrolled
// repository traffic replaces it during M4's webhook validation experiment.
func loadRecordedDeliveries(t *testing.T) []recordedDelivery {
	t.Helper()
	encoded, err := os.ReadFile("testdata/s142_recorded.json")
	if err != nil {
		t.Fatal(err)
	}
	var deliveries []recordedDelivery
	if err := json.Unmarshal(encoded, &deliveries); err != nil {
		t.Fatal(err)
	}
	if len(deliveries) < 25 || len(deliveries) > 35 {
		t.Fatalf("recorded fixture has %d deliveries, want about 30", len(deliveries))
	}
	return deliveries
}

func TestRecordedReplayDecisionsAreOrderIndependent(t *testing.T) {
	deliveries := loadRecordedDeliveries(t)
	classifier := DefaultClassifier()
	baseline := classifyDecisionSet(t, classifier, deliveries)
	if len(baseline) != 12 {
		t.Fatalf("baseline has %d unique decisions, want 12: %#v", len(baseline), baseline)
	}
	for _, required := range []Intent{
		{
			Kind: queue.KindRefreshPR, Key: "pr:acme/monolith:4800",
			Priority: PriorityEvent,
		},
		{
			Kind: queue.KindRefreshStack, Key: "stack:acme/monolith:142",
			Priority: PriorityEvent,
		},
		{
			Kind: queue.KindRefreshChecks, Key: "checks:acme/monolith:8f31c2d",
			Priority: PriorityEvent,
		},
		{
			Kind:     queue.KindRefreshBranch,
			Key:      "branch:acme/monolith:refactor/bm25f-ranker",
			Priority: PriorityEvent,
		},
	} {
		if _, ok := baseline[decisionID(required)]; !ok {
			t.Fatalf("baseline is missing required decision %+v", required)
		}
	}

	for seed := int64(0); seed < 50; seed++ {
		random := rand.New(rand.NewSource(seed)) //nolint:gosec // property permutation
		permuted := append([]recordedDelivery(nil), deliveries...)
		random.Shuffle(len(permuted), func(i, j int) {
			permuted[i], permuted[j] = permuted[j], permuted[i]
		})
		withDuplicates := make([]recordedDelivery, 0, len(permuted)*2)
		for _, delivery := range permuted {
			withDuplicates = append(withDuplicates, delivery)
			if random.Intn(3) == 0 {
				withDuplicates = append(withDuplicates, delivery)
			}
		}
		got := classifyDecisionSet(t, classifier, withDuplicates)
		if !reflect.DeepEqual(got, baseline) {
			t.Fatalf("seed %d decisions differ\n got: %#v\nwant: %#v", seed, got, baseline)
		}
	}
}

func classifyDecisionSet(
	t *testing.T,
	classifier Classifier,
	deliveries []recordedDelivery,
) map[string]Intent {
	t.Helper()
	decisions := make(map[string]Intent)
	for _, delivery := range deliveries {
		intents, err := classifier.Classify(delivery.Event, delivery.Payload)
		if err != nil {
			t.Fatalf("classify %s (%s): %v", delivery.GUID, delivery.Event, err)
		}
		for _, intent := range intents {
			decisions[decisionID(intent)] = intent
		}
	}
	return decisions
}

func decisionID(intent Intent) string {
	return intent.Kind + "\x00" + intent.Key + "\x00" + intent.Priority
}
