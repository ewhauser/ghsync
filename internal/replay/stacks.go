//nolint:gocritic // Stack derivation intentionally transforms immutable pull-request snapshots by value.
package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
)

func DeriveStacks(repository Repository, pulls []PullRequest) []Stack {
	byHead := make(map[string][]PullRequest, len(pulls))
	byNumber := make(map[int]PullRequest, len(pulls))
	for _, pull := range pulls {
		headRepository := pull.Head.Repository
		if headRepository == "" {
			headRepository = repository.FullName()
		}
		if pull.Head.Ref != repository.DefaultBranch &&
			headRepository == repository.FullName() {
			key := branchKey(headRepository, pull.Head.Ref)
			byHead[key] = append(byHead[key], pull)
		}
		byNumber[pull.Number] = pull
	}
	for key := range byHead {
		sort.Slice(byHead[key], func(i, j int) bool {
			return byHead[key][i].Number < byHead[key][j].Number
		})
	}
	adjacent := make(map[int]map[int]struct{})
	parentByChild := make(map[int]int)
	for _, pull := range pulls {
		baseRepository := pull.Base.Repository
		if baseRepository == "" {
			baseRepository = repository.FullName()
		}
		parent, ok := chooseStackParent(
			pull,
			byHead[branchKey(baseRepository, pull.Base.Ref)],
		)
		if !ok {
			continue
		}
		parentByChild[pull.Number] = parent.Number
		if adjacent[parent.Number] == nil {
			adjacent[parent.Number] = make(map[int]struct{})
		}
		if adjacent[pull.Number] == nil {
			adjacent[pull.Number] = make(map[int]struct{})
		}
		adjacent[parent.Number][pull.Number] = struct{}{}
		adjacent[pull.Number][parent.Number] = struct{}{}
	}
	visited := make(map[int]bool)
	var stacks []Stack
	numbers := make([]int, 0, len(adjacent))
	for number := range adjacent {
		numbers = append(numbers, number)
	}
	sort.Ints(numbers)
	for _, number := range numbers {
		if visited[number] {
			continue
		}
		var component []int
		queue := []int{number}
		visited[number] = true
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			component = append(component, current)
			for neighbor := range adjacent[current] {
				if !visited[neighbor] {
					visited[neighbor] = true
					queue = append(queue, neighbor)
				}
			}
		}
		if len(component) < 2 {
			continue
		}
		ordered := orderStackComponent(
			component,
			parentByChild,
		)
		if len(ordered) == 0 {
			continue
		}
		bottom := byNumber[ordered[0]]
		states := make([]PullRequest, 0, len(ordered))
		for _, member := range ordered {
			states = append(states, byNumber[member])
		}
		stacks = append(stacks, Stack{
			ID:                stackID(repository.FullName(), ordered),
			Number:            ordered[0],
			Base:              bottom.Base,
			PullRequests:      ordered,
			PullRequestStates: states,
		})
	}
	sort.Slice(stacks, func(i, j int) bool {
		return stacks[i].Number < stacks[j].Number
	})
	return stacks
}

func chooseStackParent(
	child PullRequest,
	candidates []PullRequest,
) (PullRequest, bool) {
	var fallback *PullRequest
	for index := range candidates {
		candidate := candidates[index]
		if candidate.Number == child.Number {
			continue
		}
		if candidate.Head.SHA != "" &&
			candidate.Head.SHA == child.Base.SHA {
			return candidate, true
		}
		if fallback == nil ||
			(candidate.State == "open" && fallback.State != "open") ||
			(candidate.State == fallback.State &&
				candidate.UpdatedAt.After(fallback.UpdatedAt)) {
			copy := candidate
			fallback = &copy
		}
	}
	if fallback == nil {
		return PullRequest{}, false
	}
	return *fallback, true
}

func SynthesizeStackBases(
	repository Repository,
	pulls []PullRequest,
	percent float64,
	seed int64,
) ([]PullRequest, map[int]bool) {
	result := append([]PullRequest(nil), pulls...)
	synthetic := make(map[int]bool)
	if percent <= 0 || math.IsNaN(percent) || len(result) < 2 {
		return result, synthetic
	}
	percent = min(percent, 100)
	realMembers := make(map[int]bool)
	for _, stack := range DeriveStacks(repository, result) {
		for _, number := range stack.PullRequests {
			realMembers[number] = true
		}
	}
	eligible := make([]int, 0, len(result))
	for index := range result {
		if !realMembers[result[index].Number] &&
			result[index].Head.Ref != repository.DefaultBranch {
			eligible = append(eligible, index)
		}
	}
	target := int(float64(len(eligible))*percent/100 + 0.5)
	target = min(target, len(eligible))
	if target < 2 {
		return result, synthetic
	}
	random := rand.New(rand.NewSource(seed))
	random.Shuffle(len(eligible), func(i, j int) {
		eligible[i], eligible[j] = eligible[j], eligible[i]
	})
	eligible = eligible[:target]
	for start := 0; start+1 < len(eligible); {
		remaining := len(eligible) - start
		size := 2
		if remaining%2 == 1 {
			size = 3
		}
		group := eligible[start : start+size]
		sort.Slice(group, func(i, j int) bool {
			return result[group[i]].Number < result[group[j]].Number
		})
		for _, index := range group {
			result[index].Head.Repository = repository.FullName()
			result[index].Base.Repository = repository.FullName()
			synthetic[result[index].Number] = true
		}
		for offset := 1; offset < len(group); offset++ {
			parent := result[group[offset-1]]
			child := &result[group[offset]]
			child.Base = parent.Head
		}
		start += size
	}
	return result, synthetic
}

func orderStackComponent(
	component []int,
	parentByChild map[int]int,
) []int {
	inComponent := make(map[int]bool, len(component))
	children := make(map[int][]int, len(component))
	var roots []int
	for _, number := range component {
		inComponent[number] = true
	}
	for _, number := range component {
		parent, ok := parentByChild[number]
		if !ok || !inComponent[parent] {
			roots = append(roots, number)
			continue
		}
		children[parent] = append(children[parent], number)
	}
	sort.Ints(roots)
	for parent := range children {
		sort.Ints(children[parent])
	}
	ordered := make([]int, 0, len(component))
	seen := make(map[int]bool, len(component))
	var visit func(int)
	visit = func(number int) {
		if seen[number] {
			return
		}
		seen[number] = true
		ordered = append(ordered, number)
		for _, child := range children[number] {
			visit(child)
		}
	}
	for _, root := range roots {
		visit(root)
	}
	sort.Ints(component)
	for _, number := range component {
		visit(number)
	}
	return ordered
}

func branchKey(repository string, ref string) string {
	return repository + "\x00" + ref
}

func stackID(repository string, numbers []int) int64 {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%s:", repository)
	for _, number := range numbers {
		_, _ = fmt.Fprintf(hash, "%d,", number)
	}
	sum := hash.Sum(nil)
	const stackIDRange = uint64(1_000_000_000_000)
	return int64(
		stackIDRange +
			binary.BigEndian.Uint64(sum[:8])%stackIDRange,
	)
}
