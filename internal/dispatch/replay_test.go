package dispatch

import (
	"encoding/json"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"testing"
)

type recordedDelivery struct {
	GUID    string          `json:"guid"`
	Event   string          `json:"event"`
	Payload json.RawMessage `json:"payload"`
}

// This is a synthetic recording of the S-142 traffic shape. Real recorded
// enrolled-repository traffic lands in M4's webhook validation experiment.
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

// loadGoldenJobs reads a hand-authored expected list derived directly from the
// fixture. It must never call Classify or share classifier mapping helpers.
func loadGoldenJobs(t *testing.T) []Intent {
	t.Helper()
	encoded, err := os.ReadFile("testdata/s142_expected_jobs.json")
	if err != nil {
		t.Fatal(err)
	}
	var jobs []Intent
	if err := json.Unmarshal(encoded, &jobs); err != nil {
		t.Fatal(err)
	}
	sortIntents(jobs)
	return jobs
}

func TestRecordedReplayDecisionsAreOrderIndependent(t *testing.T) {
	deliveries := loadRecordedDeliveries(t)
	classifier := DefaultClassifier()
	golden := loadGoldenJobs(t)
	baseline := classifyDecisions(t, classifier, deliveries)
	if !reflect.DeepEqual(baseline, golden) {
		t.Fatalf("baseline differs from hand-authored golden\n got: %#v\nwant: %#v", baseline, golden)
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
		got := classifyDecisions(t, classifier, withDuplicates)
		if !reflect.DeepEqual(got, golden) {
			t.Fatalf("seed %d decisions differ\n got: %#v\nwant: %#v", seed, got, golden)
		}
	}
}

func classifyDecisions(
	t *testing.T,
	classifier Classifier,
	deliveries []recordedDelivery,
) []Intent {
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
	result := make([]Intent, 0, len(decisions))
	for _, intent := range decisions {
		result = append(result, intent)
	}
	sortIntents(result)
	return result
}

func decisionID(intent Intent) string {
	return intent.Kind + "\x00" + intent.Key + "\x00" + intent.Priority
}

func sortIntents(intents []Intent) {
	sort.Slice(intents, func(i, j int) bool {
		return decisionID(intents[i]) < decisionID(intents[j])
	})
}
