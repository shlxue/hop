package org.apache.hop.pipeline.transforms;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hop.core.Result;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInput;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputDialog;
import org.apache.hop.testing.HopEnv;
import org.apache.hop.testing.HopExtension;
import org.apache.hop.testing.HopSource;
import org.apache.hop.testing.SpecMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HopExtension.class)
@HopEnv(ui = SpecMode.PREVIEW)
class JsonInputTest {

  @TestTemplate
  void showTransformUi(JsonInputDialog dialog) {
    assertNotNull(dialog);
  }

  @TestTemplate
  @HopSource
  void testJsonInput(Result rs, JsonInput transform) {
    assertNotNull(rs);
  }

  @TestTemplate
  void testJsonInputByInputField(IEngineComponent component) {}

  @Test
  void testJsonInputAndPassInputFields() {}
}
