package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/codexprovider"
	"github.com/nysa-company/sf/internal/cursorprovider"
)

type modelChoice struct{ model, family string }

func modelChoices(provider, excludedFamily string) []modelChoice {
	var models []string
	var family func(string) (string, bool)
	switch provider {
	case "codex":
		models, family = codexprovider.SupportedModels(), codexprovider.ModelFamily
	case "claude":
		models, family = claudeprovider.SupportedModels(), claudeprovider.ModelFamily
	case "cursor":
		models, family = cursorprovider.SupportedModels(), cursorprovider.ModelFamily
	default:
		return nil
	}
	var choices []modelChoice
	for _, model := range models {
		if value, ok := family(model); ok && value != excludedFamily {
			choices = append(choices, modelChoice{model, value})
		}
	}
	return choices
}

func validateExplicitModel(provider, model string) error {
	for _, choice := range modelChoices(provider, "") {
		if choice.model == model {
			return nil
		}
	}
	return errors.New("unsupported exact model/provider selection; use --models select to see the supported catalog; every model requires qualification")
}

// Selection is inert until both answers succeed. Runtime/account qualification
// and independence remain daemon/Store authority, not this catalog filter.
func (a *app) selectProviderModels(ctx context.Context, builder, reviewer string) (string, string, error) {
	if !a.canSelectInteractively() {
		return "", "", errors.New("model selection requires a terminal; use --builder-model and --reviewer-model for scripts or JSON")
	}
	builders, reviewers := modelChoices(builder, ""), modelChoices(reviewer, "")
	if len(builders) == 0 || len(reviewers) == 0 {
		return "", "", errors.New("provider has no qualified-runtime selection path; no request made")
	}
	if _, err := fmt.Fprintln(a.errOut, "Choose exact models. This is SF's supported catalog, not proof of login, entitlement, or qualification.\nAfter both selections, qualification may invoke paid models. Use q at either prompt to cancel without a request."); err != nil {
		return "", "", err
	}
	pick := func(role, provider string, choices []modelChoice) (modelChoice, error) {
		if ctx.Err() != nil {
			return modelChoice{}, ctx.Err()
		}
		if len(choices) == 0 {
			return modelChoice{}, errors.New("no independent model family for this pair; no request made")
		}
		if _, err := fmt.Fprintf(a.errOut, "%s (%s):\n", role, provider); err != nil {
			return modelChoice{}, err
		}
		for i, choice := range choices {
			if _, err := fmt.Fprintf(a.errOut, "%d) %s — %s\n", i+1, choice.model, choice.family); err != nil {
				return modelChoice{}, err
			}
		}
		if _, err := fmt.Fprint(a.errOut, "Number, or q to cancel: "); err != nil {
			return modelChoice{}, err
		}
		reader := a.input
		if reader == nil {
			reader = os.Stdin
		}
		answer, err := readSelectionAnswer(reader)
		if err != nil || ctx.Err() != nil {
			return modelChoice{}, errors.New("model selection cancelled; no request made")
		}
		n, err := strconv.Atoi(strings.TrimSpace(answer))
		if err != nil || n < 1 || n > len(choices) {
			return modelChoice{}, errors.New("model selection cancelled or invalid; no request made")
		}
		return choices[n-1], nil
	}
	b, err := pick("Planner/Builder", builder, builders)
	if err != nil {
		return "", "", err
	}
	r, err := pick("Independent Reviewer", reviewer, modelChoices(reviewer, b.family))
	if err != nil {
		return "", "", err
	}
	return b.model, r.model, nil
}
