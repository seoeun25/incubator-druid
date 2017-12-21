package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.yahoo.sketches.quantiles.ItemsSketch;

/**
 */
public enum SketchQuantilesOp
{
  QUANTILES {
    @Override
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (parameter == null) {
        return sketch.getQuantiles(DEFAULT_FRACTIONS);
      } else if (parameter instanceof double[]) {
        return sketch.getQuantiles((double[])parameter);
      } else if (parameter instanceof QuantileParam) {
        QuantileParam quantileParam = (QuantileParam) parameter;
        if (quantileParam.evenSpaced) {
          return sketch.getQuantiles(quantileParam.number); // even spaced
        }
        if (quantileParam.evenCounted) {
          final int limitNum = quantileParam.number;
          double[] quantiles = new double[(int) (sketch.getN() / limitNum) + 1];
          for (int i = 1; i < quantiles.length - 1; i++) {
            quantiles[i] = (double) limitNum * i / sketch.getN();
          }
          quantiles[quantiles.length - 1] = 1;
          return sketch.getQuantiles(quantiles);
        }
        if (quantileParam.slopedSpaced) {
          // sigma(0 ~ p) of (ax + b) = total
          // ((p * (p + 1)) / 2) * a + (p + 1) * b = total
          // a = 2 * (total - (p + 1) * b) / (p * (p + 1))
          int p = quantileParam.number - 2;
          double total = sketch.getN();
          double b = total  / (p * 3);
          double a = (total - (p + 1) * b) * 2 / (p * (p + 1));
          double[] quantiles = new double[quantileParam.number];
          for (int i = 1; i <= p; i++) {
            quantiles[i] = (i > 1 ? quantiles[i - 1] : 0) + a * (i - 1) + b;
          }
          for (int i = 1; i <= p; i++) {
            quantiles[i] /= total;
          }
          quantiles[0] = 0;
          quantiles[quantiles.length - 1] = 1;
          return sketch.getQuantiles(quantiles);
        }
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
    }
  },
  CDF {
    @Override
    @SuppressWarnings("unchecked")
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (parameter.getClass().isArray()) {
        return sketch.getCDF((Object[]) parameter);
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
    }
  },
  PMF {
    @Override
    @SuppressWarnings("unchecked")
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (parameter.getClass().isArray()) {
        return sketch.getPMF((Object[]) parameter);
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
    }
  },
  IQR {
    @Override
    @SuppressWarnings("unchecked")
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      Object[] quantiles = sketch.getQuantiles(new double[] {0.25f, 0.75f});
      if (quantiles[0] instanceof Number && quantiles[1] instanceof Number) {
        return ((Number)quantiles[1]).doubleValue() - ((Number)quantiles[0]).doubleValue();
      }
      throw new IllegalArgumentException("IQR is possible only for numeric types");
    }
  };

  public abstract Object calculate(ItemsSketch sketch, Object parameter);

  @JsonValue
  public String getName()
  {
    return name();
  }

  @JsonCreator
  public static SketchQuantilesOp fromString(String name)
  {
    return name == null ? null : valueOf(name.toUpperCase());
  }

  public static final double[] DEFAULT_FRACTIONS = new double[]{0d, 0.25d, 0.50d, 0.75d, 1.0d};

  public static final QuantileParam DEFAULT_QUANTILE_PARAM = evenSpaced(11);

  private static class QuantileParam
  {
    final int number;
    final boolean evenSpaced;
    final boolean evenCounted;
    final boolean slopedSpaced;

    private QuantileParam(int number, boolean evenSpaced, boolean evenCounted, boolean slopedSpaced) {
      this.number = number;
      this.evenSpaced = evenSpaced;
      this.evenCounted = evenCounted;
      this.slopedSpaced = slopedSpaced;
    }
  }

  public static QuantileParam evenSpaced(int partition)
  {
    return new QuantileParam(partition, true, false, false);
  }

  public static QuantileParam evenCounted(int partition)
  {
    return new QuantileParam(partition, false, true, false);
  }

  public static QuantileParam slopedSpaced(int partition)
  {
    return new QuantileParam(partition, false, false, true);
  }
}