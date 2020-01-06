package org.aksw.deer.plugin.slipo.decorator;

import org.aksw.deer.DeerAnalyticsStore;
import org.aksw.deer.DeerExecutionNode;
import org.aksw.deer.ParameterizedDeerExecutionNode;
import org.aksw.deer.decorators.AbstractDeerExecutionNodeDecorator;
import org.aksw.deer.decorators.AbstractParameterizedDeerExecutionNodeDecorator;
import org.aksw.deer.decorators.AbstractParameterizedDeerExecutionNodeWrapper;
import org.aksw.faraday_cage.engine.ExecutionNode;
import org.aksw.faraday_cage.engine.FaradayCageContext;
import org.aksw.faraday_cage.engine.Parameterized;
import org.aksw.faraday_cage.engine.ValidatableParameterMap;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.*;
import org.json.JSONObject;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
@Extension
public class SlipoAnalyticsWrapper extends AbstractParameterizedDeerExecutionNodeWrapper {

  private static final String SLIPO_NS = "http://slipo.eu/def#";

  public static final Property GLOBAL_INPUT_ID= ResourceFactory.createProperty(SLIPO_NS + "globalInputId");

  public static final Property GLOBAL_OUTPUT_ID= ResourceFactory.createProperty(SLIPO_NS + "globalOutputId");

  private static final Logger logger = LoggerFactory.getLogger(SlipoAnalyticsWrapper.class);

  private static final Map<String, Model> inputPerJob = new HashMap<>();

  private static final Property category = ResourceFactory.createProperty(SLIPO_NS + "category");

  private synchronized void addGlobalIn(Model in) {
    inputPerJob.put(FaradayCageContext.getRunId(), in);
  }

  private void addGlobalOut(Model out) {
    Model in = inputPerJob.get(FaradayCageContext.getRunId());

    Set<Resource> inPOIs = in.listStatements(null, category, (RDFNode) null)
      .mapWith(Statement::getSubject)
      .filterKeep(r -> r.hasProperty(in.createProperty(SLIPO_NS + "category")))
      .toSet();
    long avgDiff = 0;
    long maxDiff = 0;
    long enrichedPOIs = 0;
    for (Resource inPOI : inPOIs) {
      Query query = QueryFactory.create(
        "prefix x: <urn:ex:>\n" +
          "\n" +
          "select (count(?o) AS ?cnt) \n" +
          "where {\n" +
          "  <" + inPOI.getURI() + "> (x:|!x:)* ?s .\n" +
          "  ?s ?p ?o .\n" +
          "}");
      QueryExecution queryExecution = QueryExecutionFactory.create(query, in);
      long inProperties = queryExecution.execSelect().next().get("?cnt").asLiteral().getLong();
      queryExecution.close();
      queryExecution = QueryExecutionFactory.create(query, out);
      long outProperties = queryExecution.execSelect().next().get("?cnt").asLiteral().getLong();
      queryExecution.close();
      long diff = outProperties - inProperties;
      avgDiff += diff;
      if (diff > 0) {
        enrichedPOIs++;
        if (diff > maxDiff) {
          maxDiff = diff;
        }
      }
      in.listStatements(inPOI, null, (RDFNode) null).toList().size();
    }
    JSONObject root = new JSONObject();
    // compute avg Delta of enriched POIs
    root.put("enrichmentPercentage", 100d/in.size()*out.size()-100d);
    root.put("totalEnrichedTriples", avgDiff);
    root.put("avgEnrichedTriples", Math.round((double) avgDiff / (double) inPOIs.size()));
    // compute max Delta of enriched POIs
    root.put("maxEnrichedTriples", maxDiff);
    // compute number of total POIs
    root.put("totalPOIs", inPOIs.size());
    // compute number of enriched POIs
    root.put("enrichedPOIs", enrichedPOIs);
    DeerAnalyticsStore.write(FaradayCageContext.getRunId(), null, root);
    // clear reference
    inputPerJob.put(FaradayCageContext.getRunId(), null);
  }

  private void addStats(List<Model> in, String key, Resource id) {
    final List<Double> triples = new ArrayList<>(in.size());
    final List<Long> pois = new ArrayList<>(in.size());
    final List<Long> slipo = new ArrayList<>(in.size());
    for (Model m : in) {
      long poiCount = m.listStatements(null, category, (RDFNode) null).toModel().size();
      pois.add(poiCount);
      // if dataset has POIs, add up number of total triples
      if (poiCount > 0) {
        triples.add(((double)m.size())/((double)poiCount));
      } else {
        triples.add(0d);
      }
      AtomicLong slipoCounter = new AtomicLong(0);
      m.listStatements()
        .filterKeep(stmt -> stmt.getPredicate().getURI().startsWith(SLIPO_NS))
        .forEachRemaining(stmt -> slipoCounter.incrementAndGet());
      slipo.add(slipoCounter.get());
    }
    if (pois != null && pois.size() > 0 && !(pois.size() == 1 && pois.get(0) == 0)) {
      JSONObject root = new JSONObject();
      JSONObject group = new JSONObject();
      root.put(key + "Stats", group);
      group.put("numberPOIs", pois);
      group.put("avgTriples", triples);
      group.put("slipoProperties", slipo);
      DeerAnalyticsStore.write(FaradayCageContext.getRunId(), id, root);
    }
  }

  @Override
  public DeerExecutionNode wrap(DeerExecutionNode executionNode) {
    if (executionNode instanceof Parameterized) {
      return new ParameterizedSlipoAnalyticsDecorator((ParameterizedDeerExecutionNode) executionNode);
    } else {
      return new SlipoAnalyticsDecorator(executionNode);
    }
  }

  @Override
  public ValidatableParameterMap createParameterMap() {
    return ValidatableParameterMap.builder()
      .declareProperty(GLOBAL_INPUT_ID)
      .declareProperty(GLOBAL_OUTPUT_ID)
      .build();
  }

  private class SlipoAnalyticsDecorator extends AbstractDeerExecutionNodeDecorator {

    public SlipoAnalyticsDecorator(ExecutionNode<Model> other) {
      super(other);
    }

    public List<Model> apply(List<Model> in) {
      List<Model> out = super.apply(in);
      String globalIn = SlipoAnalyticsWrapper.this.getParameterMap().getOptional(GLOBAL_INPUT_ID).map(RDFNode::asResource).map(Resource::getURI).orElse("");
      String globalOut = SlipoAnalyticsWrapper.this.getParameterMap().getOptional(GLOBAL_OUTPUT_ID).map(RDFNode::asResource).map(Resource::getURI).orElse("");
      if (globalIn.equals(getId().getURI())) {
        addGlobalIn(out.get(0));
      } else if (globalOut.equals(getId().getURI())) {
        addGlobalOut(in.get(0));
      }
      addStats(in, "slipoInput", getId());
      addStats(out, "slipoOutput", getId());
      return out;
    }

  }

  private class ParameterizedSlipoAnalyticsDecorator extends AbstractParameterizedDeerExecutionNodeDecorator {

    public ParameterizedSlipoAnalyticsDecorator(ParameterizedDeerExecutionNode other) {
      super(other);
    }

    public List<Model> apply(List<Model> in) {
      List<Model> out = super.apply(in);
      String globalIn = SlipoAnalyticsWrapper.this.getParameterMap().getOptional(GLOBAL_INPUT_ID).map(RDFNode::asResource).map(Resource::getURI).orElse("");
      String globalOut = SlipoAnalyticsWrapper.this.getParameterMap().getOptional(GLOBAL_OUTPUT_ID).map(RDFNode::asResource).map(Resource::getURI).orElse("");
      if (globalIn.equals(getId().getURI())) {
        addGlobalIn(out.get(0));
      } else if (globalOut.equals(getId().getURI())) {
        addGlobalOut(in.get(0));
      }
      addStats(in, "slipoInput", getId());
      addStats(out, "slipoOutput", getId());
      return out;
    }
  }

  @Override
  public Resource getType() {
    return ResourceFactory.createResource(SLIPO_NS + this.getClass().getSimpleName());
  }
}
