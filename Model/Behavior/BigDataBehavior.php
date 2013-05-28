<?php
/**
 * BigDataBehavior v2.0
 *
 * @author Jarriett K Robinson, jarriett@gmail.com
 * @author (modifications) J. Miller, http://github.com/jmillerdesign
 */
class BigDataBehavior extends ModelBehavior {

/**
 * Setup this behavior with the specified configuration settings.
 *
 * @param Model $model Model using this behavior
 * @param array $config Configuration settings for $model
 * @return void
 */
	public function setup(Model &$Model, $config = array()) {
		// Current model
		$this->_Model = $Model;

		// Bundle of items to save
		$this->_Model->_bundle = array();
	}

/**
 * Add an array to the bundle, to be saved later
 *
 * @param Model $Model Model using this behavior
 * @param array $modelData Data to be saved
 * @return void
 */
	public function addToBundle(Model &$Model, $modelData) {
		$this->_Model = $Model;

		// Remove model name from array, if it exists
		// Append item to the bundle array
		if (array_key_exists($Model->name, $modelData)) {
			$this->_Model->_bundle[] = $this->_prepareItemForSaving($modelData[$Model->name]);
		} else {
			$this->_Model->_bundle[] = $this->_prepareItemForSaving($modelData);
		}
	}

/**
 * Save all items in the bundle, then reset the bundle
 *
 * @param Model $Model Model using this behavior
 * @param integer $maxPayload Maximum number of items to save per query
 * @param boolean $replace True to replace data, if it already exists
 * @return void
 */
	public function saveBundle(Model &$Model, $maxPayload = 10000, $replace = true) {
		$this->_Model = $Model;

		if (count($this->_Model->_bundle) > $maxPayload) {
			$chunks = array_chunk($this->_Model->_bundle, $maxPayload);
			foreach ($chunks as $chunk) {
				$this->_bulkSave($chunk, $replace);
			}
		} else {
			$this->_bulkSave($this->_Model->_bundle, $replace);
		}

		$this->_Model->_bundle = array();
	}

/**
 * Perform the query to save a large bundle of items
 *
 * @param array $bundleItems Items to save
 * @param boolean $replace True to replace data, if it already exists
 * @return void
 */
	protected function _bulkSave(&$bundleItems, $replace = true) {
		if (!$bundleItems) {
			return;
		}

		$table = Inflector::tableize($this->_Model->name);
		$fieldNames = array_keys($this->_Model->schema());

		$sql = sprintf('INSERT INTO `%s` (%s) VALUES', $table, implode(',', $fieldNames));

		foreach ($bundleItems as $bundleItem) {
			$sql .= sprintf('(%s),', implode(',', array_values($bundleItem)));
		}

		// Remove last comma
		$sql = substr($sql, 0, strlen($sql) - 1);

		if ($replace) {
			$sql .= ' ON DUPLICATE KEY UPDATE ';
			foreach ($fieldNames as $fieldName) {
				$sql .= sprintf('%s=VALUES(%s),', $fieldName, $fieldName);
			}

			// Remove last comma
			$sql = substr($sql, 0, strlen($sql) - 1);
		}

		$sql .= ';';

		$this->_Model->query($sql);
	}

/**
 * Get the empty value to use for a field
 *
 * @param array $fieldSchema Field schema
 * @return mixed Default value
 */
	protected function _generateEmptyValue($fieldSchema) {
		if ($fieldSchema['null']) {
			return 'NULL';
		}

		if (!$fieldSchema['default']) {
			switch ($fieldSchema['type']) {
				case 'string':   return '';
				case 'date':     return date('Y-m-d');
				case 'datetime': return date('Y-m-d H:i:s');
				default:         return 0;
			}
		}

		return $fieldSchema['default'];
	}

/**
 * Add default values for keys that exist in the schema but are not set in the data.
 * Will not insert the primary key, since the database will handle that.
 *
 * @param array $modelData Data to be saved
 * @return array Data to be saved
 */
	protected function _prepareItemForSaving($modelData) {
		foreach ($this->_Model->schema() as $fieldName => $fieldSchema) {
			if (!array_key_exists($fieldName, $modelData)) {
				// Schema exists, but is not set in the model data
				// Insert the default value, unless it is the primary key
				if (!array_key_exists('key', $fieldSchema) || ($fieldSchema['key'] != 'primary')) {
					$modelData[$fieldName] = $this->_generateEmptyValue($fieldSchema);
				}
			}

			// Wrap values in quotes, for SQL string
			if ($modelData[$fieldName] != 'NULL') {
				$modelData[$fieldName] = '"' . $modelData[$fieldName] . '"';
			}
		}

		return $modelData;
	}

}
