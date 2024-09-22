using System;
using System.Linq;
using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.ML.Trainers;
using Microsoft.ML.Trainers.FastTree;

namespace Coflnet.Sky.Sniper.Services;

public class AiFinder
{
    public void Train()
    {
        var mlContext = new MLContext();

        // Load the as float array per line
        var dataView = mlContext.Data.LoadFromTextFile<RegressionData>(
            path: "data2.csv",
            hasHeader: false,
            separatorChar: ',');
        // Split the data into train and test sets
        var splitData = mlContext.Data.TrainTestSplit(dataView, testFraction: 0.2);
        var trainingData = splitData.TrainSet;
        var testData = splitData.TestSet;

        Console.WriteLine($"starting training on {trainingData.GetRowCount()} samples");
        // Create the pipeline
        var pipeline = mlContext.Transforms.Concatenate("Features", nameof(RegressionData.Features))
            // Normalize the data
            // add dense relu layer
            .Append(mlContext.Regression.Trainers.FastForest(new FastForestRegressionTrainer.Options(){
                NumberOfTrees=70,NumberOfLeaves=40,FeatureFraction=1F,LabelColumnName=@"Label",FeatureColumnName=@"Features"}));

        // Train the model
        var model = pipeline.Fit(trainingData);

        // Evaluate the model
        var predictions = model.Transform(testData);
        var metrics = mlContext.Regression.Evaluate(predictions, labelColumnName: "Label", scoreColumnName: "Score");

        Console.WriteLine($"R-Squared: {metrics.RSquared}");
        Console.WriteLine($"Mean Absolute Error: {metrics.MeanAbsoluteError}");
        Console.WriteLine($"Root Mean Squared Error: {metrics.RootMeanSquaredError}");

        // Use the model for prediction
        var predictionEngine = mlContext.Model.CreatePredictionEngine<RegressionData, RegressionPrediction>(model);

        // Example prediction
        var newSample = new RegressionData
        {
            // 2_011_392_409
            Features = new float[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0.016391657f, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0018168028f, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0 }
        };

        var prediction = predictionEngine.Predict(newSample);
        Console.WriteLine($"Predicted Value: {prediction.PredictedValue}");
    }
}

public class RegressionData
{
    [LoadColumn(0)] public float Label;  // The target value (first column)
    [LoadColumn(1, 392)][VectorType(392)] public float[] Features;  // All remaining columns are features
}

public class RegressionPrediction
{
    [ColumnName("Score")] public float PredictedValue;  // The predicted value
}
#nullable disable
