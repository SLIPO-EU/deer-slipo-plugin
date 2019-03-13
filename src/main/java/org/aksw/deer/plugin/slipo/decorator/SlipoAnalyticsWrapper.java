package org.aksw.deer.plugin.slipo.decorator;

import org.aksw.deer.DeerAnalyticsStore;
import org.aksw.deer.DeerExecutionNode;
import org.aksw.deer.ParameterizedDeerExecutionNode;
import org.aksw.deer.decorators.AbstractDeerExecutionNodeDecorator;
import org.aksw.deer.decorators.AbstractDeerExecutionNodeWrapper;
import org.aksw.deer.decorators.AbstractParameterizedDeerExecutionNodeDecorator;
import org.aksw.deer.decorators.SparqlAnalyticsWrapper;
import org.aksw.faraday_cage.engine.ExecutionNode;
import org.aksw.faraday_cage.engine.FaradayCageContext;
import org.aksw.faraday_cage.engine.Parameterized;
import org.apache.jena.rdf.model.*;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
@Extension
public class SlipoAnalyticsWrapper extends AbstractDeerExecutionNodeWrapper {

  private static final String SLIPO_NS = "http://slipo.eu/def#";

  private static final Logger logger = LoggerFactory.getLogger(SparqlAnalyticsWrapper.class);

  private void addStats(List<Model> in, String key, Resource id) {
    final Property category = ResourceFactory.createProperty(SLIPO_NS + "category");
    final List<Double> triples = new ArrayList<>(in.size());
    final List<Long> pois = new ArrayList<>(in.size());
    for (Model m : in) {
      long poiCount = m.listStatements(null, category, (RDFNode) null).toModel().size();
      pois.add(poiCount);
      // if dataset has POIs, add up number of total triples
      if (poiCount > 0) {
        triples.add(((double)m.size())/((double)poiCount));
      } else {
        triples.add(0d);
      }
    }
    JSONObject stats = new JSONObject();
    stats.put("number of POIs in " + key, pois);
    stats.put("triples per POI in " + key, triples);
    DeerAnalyticsStore.write(FaradayCageContext.getRunId(), id, stats);
  }

  @Override
  public DeerExecutionNode wrap(DeerExecutionNode executionNode) {
    if (executionNode instanceof Parameterized) {
      return new ParameterizedSlipoAnalyticsDecorator((ParameterizedDeerExecutionNode) executionNode);
    } else {
      return new SlipoAnalyticsDecorator(executionNode);
    }
  }

  private class SlipoAnalyticsDecorator extends AbstractDeerExecutionNodeDecorator {

    public SlipoAnalyticsDecorator(ExecutionNode<Model> other) {
      super(other);
    }

    public List<Model> apply(List<Model> in) {
      List<Model> out = super.apply(in);
      addStats(in, "input", getId());
      addStats(out, "output", getId());
      return out;
    }

  }

  private class ParameterizedSlipoAnalyticsDecorator extends AbstractParameterizedDeerExecutionNodeDecorator {

    public ParameterizedSlipoAnalyticsDecorator(ParameterizedDeerExecutionNode other) {
      super(other);
    }

    public List<Model> apply(List<Model> in) {
      List<Model> out = super.apply(in);
      if (in.size() > 0) {
        addStats(in, "input", getId());
      }
      if (out.size() > 0) {
        addStats(out, "output", getId());
      }
      return out;
    }

  }

  @NotNull
  @Override
  public Resource getType() {
    return ResourceFactory.createResource(SLIPO_NS + this.getClass().getSimpleName());
  }
}
