package store

import (
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestProjectConfigurationHistoryAndPointerAreAppendOnly(t *testing.T) {
	db, ctx := openTestStore(t)
	defer db.Close()

	project := testConfigurationProject(t, "config-authority", "/tmp/config-authority", 1)
	if err := db.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	proposal := nextProjectConfiguration(t, project, 2)
	if _, observed, err := db.ApplyProjectConfiguration(ctx, proposal); err != nil || observed {
		t.Fatalf("append generation: observed=%v err=%v", observed, err)
	}

	for name, statement := range map[string]string{
		"update generation row": `UPDATE project_configurations SET snapshot_bytes=? WHERE channel=? AND project_id=? AND generation=1`,
		"delete generation row": `DELETE FROM project_configurations WHERE channel=? AND project_id=? AND generation=1`,
	} {
		t.Run(name, func(t *testing.T) {
			var err error
			if name == "update generation row" {
				_, err = db.db.ExecContext(ctx, statement, project.ConfigSnapshot, project.Channel, project.ID)
			} else {
				_, err = db.db.ExecContext(ctx, statement, project.Channel, project.ID)
			}
			if err == nil {
				t.Fatal("immutable generation mutation was accepted")
			}
		})
	}

	for name, generation := range map[string]uint64{"rollback": 0, "arbitrary jump": 4} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.db.ExecContext(ctx, `UPDATE projects SET current_config_generation=? WHERE channel=? AND id=?`, generation, project.Channel, project.ID); err == nil {
				t.Fatal("invalid configuration pointer mutation was accepted")
			}
		})
	}
	loaded, err := db.Project(ctx, domain.ChannelDev, project.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ConfigGeneration != 2 || loaded.ConfigDigest != proposal.Digest {
		t.Fatalf("invalid mutation changed pointer: %+v", loaded)
	}
}

func TestProjectConfigurationPointerAllowsOnlyExistingNextGeneration(t *testing.T) {
	db, ctx := openTestStore(t)
	defer db.Close()
	project := testConfigurationProject(t, "config-pointer", "/tmp/config-pointer", 1)
	if err := db.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	proposal := nextProjectConfiguration(t, project, 2)
	if _, _, err := db.ApplyProjectConfiguration(ctx, proposal); err != nil {
		t.Fatal(err)
	}
	// A direct writer may advance to the already-authenticated next generation;
	// it may not skip over it or point at a nonexistent generation.
	if _, err := db.db.ExecContext(ctx, `UPDATE projects SET current_config_generation=2 WHERE channel=? AND id=?`, project.Channel, project.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE projects SET current_config_generation=4 WHERE channel=? AND id=?`, project.Channel, project.ID); err == nil {
		t.Fatal("pointer skipped a generation")
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE projects SET current_config_generation=1 WHERE channel=? AND id=?`, project.Channel, project.ID); err == nil {
		t.Fatal("pointer rolled back a generation")
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE projects SET current_config_generation=3 WHERE channel=? AND id=?`, project.Channel, project.ID); err == nil {
		t.Fatal("pointer advanced to a missing generation")
	}
	if loaded, err := db.Project(ctx, project.Channel, project.ID); err != nil || loaded.ConfigGeneration != 2 {
		t.Fatalf("pointer=%+v err=%v", loaded, err)
	}
}

func TestProjectConfigurationGenerationTriggerExistsAfterV50Migration(t *testing.T) {
	db, ctx := openTestStore(t)
	defer db.Close()
	for _, trigger := range []string{
		"project_configurations_immutable_update",
		"project_configurations_immutable_delete",
		"projects_config_generation_forward",
	} {
		var count int
		if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=?`, trigger).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("trigger %s count=%d", trigger, count)
		}
	}
	if err := db.validateSchema(ctx); err != nil {
		t.Fatal(err)
	}
}
