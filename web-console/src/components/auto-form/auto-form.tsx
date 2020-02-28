/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  Button,
  ButtonGroup,
  FileInput,
  FormGroup,
  Icon,
  Intent,
  NumericInput,
  Popover,
} from '@blueprintjs/core';
import { DateInput } from '@blueprintjs/datetime';
import { IconNames } from '@blueprintjs/icons';
import moment from 'moment';
import React, { FormEvent } from 'react';

import { parseStringToJson } from '../../utils';
import { deepDelete, deepGet, deepSet } from '../../utils/object-change';
import { ArrayInput } from '../array-input/array-input';
import { JSONInput } from '../json-input/json-input';
import { SuggestibleInput, SuggestionGroup } from '../suggestible-input/suggestible-input';

import '@blueprintjs/core/lib/css/blueprint.css';
import '@blueprintjs/datetime/lib/css/blueprint-datetime.css';
import './auto-form.scss';

export interface Field<T> {
  name: string;
  label?: string;
  info?: React.ReactNode;
  type:
    | 'number'
    | 'size-bytes'
    | 'string'
    | 'duration'
    | 'boolean'
    | 'string-array'
    | 'json'
    | 'file'
    | 'date';
  defaultValue?: any;
  suggestions?: (string | SuggestionGroup)[];
  placeholder?: string;
  min?: number;
  disabled?: boolean | ((model: T) => boolean);
  defined?: boolean | ((model: T) => boolean);
  required?: boolean | ((model: T) => boolean);
}

export interface AutoFormProps<T> {
  fields: Field<T>[];
  model: T | undefined;
  onChange: (newModel: T) => void;
  showCustom?: (model: T) => boolean;
  updateJSONValidity?: (jsonValidity: boolean) => void;
  large?: boolean;
}

export interface AutoFormState {
  jsonInputsValidity: any;
}

export class AutoForm<T extends Record<string, any>> extends React.PureComponent<
  AutoFormProps<T>,
  AutoFormState
> {
  static makeLabelName(label: string): string {
    let newLabel = label
      .split(/(?=[A-Z])/)
      .join(' ')
      .toLowerCase()
      .replace(/\./g, ' ');
    newLabel = newLabel[0].toUpperCase() + newLabel.slice(1);
    return newLabel;
  }

  static evaluateFunctor<T>(
    functor: undefined | boolean | ((model: T) => boolean),
    model: T | undefined,
    defaultValue = false,
  ): boolean {
    if (!model || functor == null) return defaultValue;
    switch (typeof functor) {
      case 'boolean':
        return functor;

      case 'function':
        return functor(model);

      default:
        throw new TypeError(`invalid functor`);
    }
  }

  constructor(props: AutoFormProps<T>) {
    super(props);
    this.state = {
      jsonInputsValidity: {},
    };
  }

  private fieldValue = (field: Field<T>): any => {
    const { model } = this.props;
    if (!model) return;
    return deepGet(model, field.name);
  };

  private fieldChange = (field: Field<T>, newValue: any) => {
    const { model } = this.props;
    if (!model) return;

    const newModel =
      typeof newValue === 'undefined'
        ? deepDelete(model, field.name)
        : deepSet(model, field.name, newValue);

    this.modelChange(newModel);
  };

  private modelChange = (newModel: T) => {
    const { fields, onChange } = this.props;

    for (const someField of fields) {
      if (!AutoForm.evaluateFunctor(someField.defined, newModel, true)) {
        newModel = deepDelete(newModel, someField.name);
      }
    }

    onChange(newModel);
  };

  private renderNumberInput(field: Field<T>): JSX.Element {
    const { model, large } = this.props;

    const modelValue = deepGet(model as any, field.name) || field.defaultValue;
    return (
      <NumericInput
        value={modelValue}
        onValueChange={(valueAsNumber: number, valueAsString: string) => {
          if (valueAsString === '') {
            this.fieldChange(field, undefined);
            return;
          }
          if (isNaN(valueAsNumber)) return;
          this.fieldChange(field, valueAsNumber);
        }}
        min={field.min || 0}
        fill
        large={large}
        disabled={AutoForm.evaluateFunctor(field.disabled, model)}
        placeholder={field.placeholder}
        intent={
          AutoForm.evaluateFunctor(field.required, model) && modelValue == null
            ? Intent.PRIMARY
            : undefined
        }
      />
    );
  }

  private renderSizeBytesInput(field: Field<T>): JSX.Element {
    const { model, large } = this.props;
    return (
      <NumericInput
        value={deepGet(model as any, field.name) || field.defaultValue}
        onValueChange={(v: number) => {
          if (isNaN(v)) return;
          this.fieldChange(field, v);
        }}
        min={0}
        stepSize={1000}
        majorStepSize={1000000}
        large={large}
        disabled={AutoForm.evaluateFunctor(field.disabled, model)}
      />
    );
  }

  private renderStringInput(field: Field<T>, sanitize?: (str: string) => string): JSX.Element {
    const { model, large } = this.props;

    const modalValue = deepGet(model as any, field.name);
    return (
      <SuggestibleInput
        value={modalValue != null ? modalValue : field.defaultValue || ''}
        onValueChange={v => {
          if (sanitize) v = sanitize(v);
          this.fieldChange(field, v);
        }}
        onBlur={() => {
          if (modalValue === '') this.fieldChange(field, undefined);
        }}
        placeholder={field.placeholder}
        suggestions={field.suggestions}
        large={large}
        disabled={AutoForm.evaluateFunctor(field.disabled, model)}
      />
    );
  }

  private renderDateInput(field: Field<T>): JSX.Element {
    const { model } = this.props;
    const fieldValue = deepGet(model as any, field.name);
    const placeholderValue = 'Date & Time (UTC)';
    const precision = 'minute'; // other options 'second', 'millisenods'
    // <DateInput> selectors (month and year) are too narrow, updating original padding from { padding: 0 25px 0 10px; }
    const style = `.bp3-html-select select { padding: 0 5px 0 5px; }`;
    // input field width is also too small and doesn't fit default date format values
    const inputProps = { style: { width: '15em' } };
    // return is wrapped with div to prevent styling issues with labels and <DateInput> (simplest/fastest solution)
    return (
      <div>
        <style>{style}</style>
        <DateInput
          inputProps={inputProps}
          placeholder={placeholderValue}
          formatDate={(date: Date) => this.formatDate(date)}
          timePrecision={precision}
          parseDate={(data: string) => this.parseDate(data)}
          onChange={(v: any) => this.onDateInputChange(field, v)}
          value={this.getDateValue(fieldValue)}
        />
      </div>
    );
  }

  private formatDate(date: Date): string {
    return moment(date).format('YYYY-MM-DD HH:mm:ss');
  }

  private getDateValue(fieldValue: any): Date | null {
    return fieldValue ? this.parseDate(fieldValue) : null;
  }

  private parseDate(data: string): Date {
    // have to adjust for local timezone manually because DateInput component doesn't have timezone support
    // https://github.com/palantir/blueprint/issues/890

    // set UTC offset to match local timezone
    const dateWithOffset = this.setUtcOffset(moment.utc(data), moment(data).utcOffset());
    return dateWithOffset.toDate();
  }

  private onDateInputChange(field: Field<T>, value: any): void {
    // set UTC offset to 0 since backend operates on UTC dates
    const dateUtc = this.setUtcOffset(moment(value), 0);
    this.fieldChange(field, dateUtc.toISOString());
  }

  private setUtcOffset(date: moment.Moment, utcOffset: number): moment.Moment {
    return date.utcOffset(utcOffset, true);
  }

  private renderBooleanInput(field: Field<T>): JSX.Element {
    const { model, large } = this.props;
    let curValue = deepGet(model as any, field.name);
    if (curValue == null) curValue = field.defaultValue;
    const disabled = AutoForm.evaluateFunctor(field.disabled, model);
    return (
      <ButtonGroup large={large}>
        <Button
          disabled={disabled}
          active={!curValue}
          onClick={() => this.fieldChange(field, false)}
        >
          False
        </Button>
        <Button disabled={disabled} active={curValue} onClick={() => this.fieldChange(field, true)}>
          True
        </Button>
      </ButtonGroup>
    );
  }

  private renderJSONInput(field: Field<T>): JSX.Element {
    const { model, updateJSONValidity } = this.props;
    const { jsonInputsValidity } = this.state;

    const updateInputValidity = (e: any) => {
      if (updateJSONValidity) {
        const newJSONInputValidity = Object.assign({}, jsonInputsValidity, { [field.name]: e });
        this.setState({
          jsonInputsValidity: newJSONInputValidity,
        });
        const allJSONValid: boolean = Object.keys(newJSONInputValidity).every(
          property => newJSONInputValidity[property] === true,
        );
        updateJSONValidity(allJSONValid);
      }
    };

    return (
      <JSONInput
        value={deepGet(model as any, field.name)}
        onChange={(v: any) => this.fieldChange(field, v)}
        updateInputValidity={updateInputValidity}
        placeholder={field.placeholder}
      />
    );
  }

  private renderFileInput(field: Field<T>): JSX.Element {
    const reader = new FileReader();
    reader.onload = (event: ProgressEvent) => {
      if (!event.target) return;
      const target: FileReader = event.target as FileReader;
      if (typeof target.result !== 'string') return;
      const newValue = parseStringToJson(target.result);
      this.fieldChange(field, newValue);
    };
    const currentValue = this.fieldValue(field);
    return (
      <FileInput
        buttonText={'Browse'}
        text={currentValue ? JSON.stringify(currentValue) : field.placeholder}
        inputProps={{ accept: '.json' }}
        onInputChange={(e: FormEvent<HTMLInputElement>) => {
          e.preventDefault();
          const input: HTMLInputElement = e.target as HTMLInputElement;
          if (!input.files || input.files.length <= 0) return;
          const files: FileList = input.files as FileList;
          reader.readAsText(files[0]);
        }}
      />
    );
  }

  private renderStringArrayInput(field: Field<T>): JSX.Element {
    const { model, large } = this.props;
    const modelValue = deepGet(model as any, field.name);
    return (
      <ArrayInput
        values={modelValue || []}
        onChange={(v: any) => {
          this.fieldChange(field, v);
        }}
        placeholder={field.placeholder}
        large={large}
        disabled={AutoForm.evaluateFunctor(field.disabled, model)}
        intent={
          AutoForm.evaluateFunctor(field.required, model) && modelValue == null
            ? Intent.PRIMARY
            : undefined
        }
      />
    );
  }

  renderFieldInput(field: Field<T>) {
    switch (field.type) {
      case 'number':
        return this.renderNumberInput(field);
      case 'size-bytes':
        return this.renderSizeBytesInput(field);
      case 'string':
        return this.renderStringInput(field);
      case 'duration':
        return this.renderStringInput(field, (str: string) =>
          str.toUpperCase().replace(/[^0-9PYMDTHS.,]/g, ''),
        );
      case 'boolean':
        return this.renderBooleanInput(field);
      case 'string-array':
        return this.renderStringArrayInput(field);
      case 'json':
        return this.renderJSONInput(field);
      case 'file':
        return this.renderFileInput(field);
      case 'date':
        return this.renderDateInput(field);
      default:
        throw new Error(`unknown field type '${field.type}'`);
    }
  }

  private renderField = (field: Field<T>) => {
    const { model } = this.props;
    if (!model) return;
    if (!AutoForm.evaluateFunctor(field.defined, model, true)) return;

    const label = field.label || AutoForm.makeLabelName(field.name);
    return (
      <FormGroup
        key={field.name}
        label={label}
        labelInfo={
          field.info && (
            <Popover
              content={<div className="label-info-text">{field.info}</div>}
              position="left-bottom"
            >
              <Icon icon={IconNames.INFO_SIGN} iconSize={14} />
            </Popover>
          )
        }
      >
        {this.renderFieldInput(field)}
      </FormGroup>
    );
  };

  renderCustom() {
    const { model } = this.props;

    return (
      <FormGroup label="Custom" key="custom">
        <JSONInput value={model} onChange={this.modelChange} />
      </FormGroup>
    );
  }

  render(): JSX.Element {
    const { fields, model, showCustom } = this.props;
    return (
      <div className="auto-form">
        {model && fields.map(this.renderField)}
        {model && showCustom && showCustom(model) && this.renderCustom()}
      </div>
    );
  }
}
