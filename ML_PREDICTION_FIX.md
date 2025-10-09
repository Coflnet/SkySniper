# ML Prediction Engine Fix - Vector Size Mismatch

## Problem
The self-learning flip finder was throwing `InvalidOperationException` during prediction when new features appeared in auctions that weren't present when the model was trained.

### Root Cause
1. Models were trained with a specific number of features (e.g., 50 features)
2. The feature index was persisted and loaded from disk
3. When a new auction arrived with NEW features not in the original training data:
   - The feature index would be updated with the new features (e.g., now 52 features)
   - The prediction code would create a feature vector with size 52
   - But the ML model expected exactly 50 features
   - This mismatch caused ML.NET to throw `InvalidOperationException` in `TreeEnsembleModelParameters.Map`

## Solution
Implemented a strict vector size tracking system:

### Changes Made

1. **Added `modelVectorSizeByTag` dictionary** to track the exact feature vector size each model expects

2. **New method `CreateFeatureVectorForPrediction`**
   - Creates feature vectors with the **exact** size the model expects
   - Ignores new features that appear after training (sets them to 0)
   - Only uses features that exist in the model's feature index
   - Bounds-checks index values to prevent out-of-range errors

3. **Updated `RefitModel`**
   - Stores the feature count when training: `modelVectorSizeByTag[tag] = featureCount`
   - Logs the stored vector size for debugging

4. **Updated `LoadPersistedModelIfExists`**
   - Extracts vector size from loaded model schema
   - Stores it in `modelVectorSizeByTag[tag]`
   - Logs the loaded vector size for debugging

5. **Updated `EstimateAsync`**
   - Uses `CreateFeatureVectorForPrediction` with the stored expected vector size
   - Added safety check to verify vector size matches expected size
   - Falls back to baseline estimate if size mismatch detected

6. **Updated `ClearModelState`**
   - Removes vector size tracking when model is cleared

## Behavior

### Before Fix
- ❌ New features → vector size mismatch → `InvalidOperationException`
- ❌ Predictions fail completely
- ❌ No way to recover without retraining

### After Fix
- ✅ New features → ignored during prediction (set to 0)
- ✅ Predictions succeed with existing model
- ✅ New features will be incorporated in the next training iteration
- ✅ Graceful degradation with safety checks and logging

## Key Files Modified
- `/Services/SelfLearningFlipFinderService.cs`
  - Added `modelVectorSizeByTag` field
  - Added `CreateFeatureVectorForPrediction` method
  - Updated `EstimateAsync`, `RefitModel`, `LoadPersistedModelIfExists`, `ClearModelState`

## Testing Recommendations
1. Load a persisted model from production
2. Send an auction with a NEW feature not in the training data
3. Verify prediction succeeds (doesn't throw exception)
4. Check logs to see:
   - "Loaded persisted model for {Tag} with {VectorSize} features"
   - "Prediction for {Tag}: {Score} (baseline {Baseline})"
5. Verify NO "Feature vector size mismatch" warnings appear

## Monitoring
Watch for these log messages:
- **Info**: "Loaded persisted model for {Tag} with {VectorSize} features" - Normal operation
- **Debug**: "Stored model vector size for {Tag}: {VectorSize} features" - Normal training
- **Warning**: "Feature vector size mismatch for {Tag}: created {ActualSize}, expected {ExpectedSize}" - Should NOT appear if fix works correctly

## Future Improvements
1. Consider adding feature versioning to models
2. Implement automatic model retraining when feature drift is detected
3. Add metrics to track prediction fallback rate
