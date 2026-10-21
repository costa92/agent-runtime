package session

// Stoppable reports whether the user may stop turnID: it is the active Turn of
// an active, unparked Session, and no tool outcome awaits reconciliation. A
// tool that is still running does not prevent it; the driver waits for it.
func Stoppable(p Projection, turnID string) bool {
	if p.Status != "active" || p.ParkedReason != "" || turnID == "" || len(p.Turns) == 0 {
		return false
	}
	t := p.Turns[len(p.Turns)-1]
	if t.ID != turnID || t.Status != "active" {
		return false
	}
	if s := activeStep(t); s != nil {
		for _, tool := range s.Tools {
			if tool.Result != nil && tool.Result.Outcome == "unknown" {
				return false
			}
		}
	}
	return true
}

// ToolRunning reports whether the active Step has a tool call without a result.
func ToolRunning(p Projection) bool {
	if len(p.Turns) == 0 {
		return false
	}
	if s := activeStep(p.Turns[len(p.Turns)-1]); s != nil {
		for _, tool := range s.Tools {
			if tool.Result == nil {
				return true
			}
		}
	}
	return false
}

func activeStep(t Turn) *Step {
	if t.Status != "active" || len(t.Steps) == 0 || t.Steps[len(t.Steps)-1].Status != "active" {
		return nil
	}
	return &t.Steps[len(t.Steps)-1]
}
