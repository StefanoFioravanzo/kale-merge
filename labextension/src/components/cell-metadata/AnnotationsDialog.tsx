/*
 * Copyright 2020 The Kale Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as React from 'react';
import { MaterialInput, MaterialSelect } from '../Components';
import {
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Grid,
  Switch,
} from '@material-ui/core';
import ColorUtils from './ColorUtils';

interface AnnotationsDialog {
  open: boolean;
  stepName: string;
  annotations: { [id: string]: string };
  updateAnnotations: Function;
  toggleDialog: Function;
}

export const AnnotationsDialog: React.FunctionComponent<AnnotationsDialog> = props => {
  const handleClose = () => {
    props.toggleDialog();
  };

  const {
    gpu_enabled = 'false',
    gpu_count = '0',
    gpu_type = 'nvidia',
  } = props.annotations;

  return (
    <Dialog
      open={props.open}
      onClose={handleClose}
      fullWidth={true}
      maxWidth={'sm'}
      scroll="paper"
      aria-labelledby="scroll-dialog-title"
      aria-describedby="scroll-dialog-description"
    >
      <DialogTitle id="scroll-dialog-title">
        <Grid
          container
          direction="row"
          justify="space-between"
          alignItems="center"
        >
          <Grid item xs={9}>
            <Grid
              container
              direction="row"
              justify="flex-start"
              alignItems="center"
            >
              <p>Require GPU for step </p>
              <Chip
                className={'kale-chip'}
                style={{
                  marginLeft: '10px',
                  backgroundColor: `#${ColorUtils.getColor(props.stepName)}`,
                }}
                key={props.stepName}
                label={props.stepName}
              />
            </Grid>
          </Grid>
          <Grid item xs={3}>
            <Grid
              container
              direction="row"
              justify="flex-end"
              alignItems="center"
            >
              <Switch
                checked={gpu_enabled === 'true'}
                onChange={c => {
                  if (c.target.checked) {
                    props.updateAnnotations({
                      gpu_enabled: c.target.checked,
                      gpu_count,
                      gpu_type,
                    });
                  } else {
                    props.updateAnnotations({ gpu_enabled: c.target.checked });
                  }
                }}
                color="primary"
                name="enableKale"
                inputProps={{ 'aria-label': 'primary checkbox' }}
                classes={{ root: 'material-switch' }}
              />
            </Grid>
          </Grid>
        </Grid>
      </DialogTitle>
      <DialogContent dividers={true} style={{ paddingTop: 0 }}>
        <Grid container direction="column" justify="center" alignItems="center">
          <Grid
            container
            direction="row"
            justify="space-between"
            alignItems="center"
            style={{ marginTop: '15px' }}
          >
            <Grid item xs={6}>
              <MaterialInput
                disabled={gpu_enabled === 'false'}
                variant="outlined"
                label="GPU Count"
                value={gpu_count}
                updateValue={(v: string) =>
                  props.updateAnnotations({
                    gpu_enabled,
                    gpu_count: v,
                    gpu_type,
                  })
                }
                style={{ width: '95%' }}
              />
            </Grid>
            <Grid item xs={6}>
              <MaterialSelect
                disabled={gpu_enabled === 'false'}
                updateValue={(v: string) =>
                  props.updateAnnotations({
                    gpu_enabled,
                    gpu_count,
                    gpu_type: v,
                  })
                }
                values={GPU_TYPES}
                value={gpu_type}
                label="GPU Type"
                index={0}
                variant="outlined"
              />
            </Grid>
          </Grid>
        </Grid>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} color="primary">
          Ok
        </Button>
      </DialogActions>
    </Dialog>
  );
};
